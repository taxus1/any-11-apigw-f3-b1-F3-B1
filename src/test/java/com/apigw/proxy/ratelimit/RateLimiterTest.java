package com.apigw.proxy.ratelimit;

import com.apigw.domain.ratelimit.RateLimitRepository;
import com.apigw.domain.ratelimit.RateLimitScope;
import com.apigw.domain.ratelimit.RateQuota;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 限流器的两条硬保证（不依赖 Redis，用假存储）：
 *
 * 一、计数归属只有一份：
 * - 多个限流器实例（模拟多台网关）共享同一存储时，合计放行恰为额度，不按实例数放大；
 * - 存储故障期 fail-open 放行不计数——既不消耗全局名额，也没有任何本机小账本暗算一份
 *   （故障期放多少笔都不影响恢复后当前窗的剩余名额）。
 *
 * 二、存储故障的处理：
 * - 两层都不限：根本不调存储；
 * - 存储超时/出错：短超时后按 fail-open（默认放行）或 fail-closed（挡回）决策，不死等；
 * - 连续失败达阈值熔断打开：打开期间不再调存储（调用计数不再增长），立即决策；
 * - 半开只放一笔探活（并发下也只一笔），探活失败重新熔断，探活成功闭合并回到全局计数；
 * - fail-closed 时两层额度同一条口径：整笔挡回 503，不存在一层放一层挡。
 */
class RateLimiterTest {

    private FakeRepository quotaRepository;
    private RateLimitCatalog catalog;
    private FakeWindowStore windowStore;

    @BeforeEach
    void setUp() {
        quotaRepository = new FakeRepository();
        catalog = new RateLimitCatalog(quotaRepository);
        windowStore = new FakeWindowStore();
    }

    private RateLimiter limiter(RateLimitProperties props) {
        return new RateLimiter(catalog, windowStore, props);
    }

    private RateLimitProperties props(boolean failOpen, int threshold, long openMs) {
        return new RateLimitProperties(true, Duration.ofSeconds(10), 10_000,
                Duration.ofMillis(100), failOpen, threshold, openMs, 60);
    }

    private void givenAppQuota(Integer limit) {
        quotaRepository.rows.add(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, limit, null, null, null));
        catalog.refreshBlock(Duration.ofSeconds(5));
    }

    private RateLimiter.Gate check(RateLimiter rl, String appNo, String ip) {
        return rl.check(appNo, ip).block(Duration.ofSeconds(2));
    }

    @Test
    void nothingLimited_doesNotTouchStore() {
        // 不给 app-1 配任何额度，也没默认 → 两层都不限
        catalog.refreshBlock(Duration.ofSeconds(5));
        RateLimiter rl = limiter(props(true, 5, 5_000));
        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");

        assertThat(gate.allowed()).isTrue();
        assertThat(windowStore.calls.get()).isZero();
    }

    @Test
    void twoInstances_shareOneStore_quotaIsNotMultiplied() {
        // 核心回归：额度是全局一份。两个限流器实例 = 两台网关，打到同一存储
        givenAppQuota(5);
        RateLimiter gw1 = limiter(props(true, 5, 50_000));
        RateLimiter gw2 = limiter(props(true, 5, 50_000));

        int allowed = 0;
        for (int i = 0; i < 12; i++) {
            RateLimiter gw = (i % 2 == 0) ? gw1 : gw2;
            if (check(gw, "app-1", "1.1.1." + i).allowed()) {
                allowed++;
            }
        }
        // 两台合计放行恰为 5：绝不因实例数放大成 10
        assertThat(allowed).isEqualTo(5);
    }

    @Test
    void storeError_defaultFailOpen_allowsAndFlagsUnavailable() {
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(true, 5, 50_000));

        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");
        assertThat(gate.allowed()).isTrue();
        assertThat(gate.storeUnavailable()).isTrue();
    }

    @Test
    void failOpen_duringOutage_countsNothing_anywhere() {
        // 故障期放行的请求：不消耗全局名额，也没有本机兜底在暗处按本机另算一份
        givenAppQuota(3);
        windowStore.fail = true;
        // 阈值调高不熔断，让每一笔都走「调用失败 → fail-open」路径
        RateLimiter rl = limiter(props(true, 1_000, 50_000));

        for (int i = 0; i < 10; i++) {
            RateLimiter.Gate g = check(rl, "app-1", "1.1.1.1");
            assertThat(g.allowed()).isTrue();
            assertThat(g.storeUnavailable()).isTrue();
        }

        // 存储恢复：当前窗名额一笔没少——仍精确放 3 笔、第 4 笔挡。
        // 若故障期有本机计数，这里会表现为「恢复前后加起来超过 3」或「故障期就被本机盖挡住」
        windowStore.fail = false;
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        RateLimiter.Gate fourth = check(rl, "app-1", "1.1.1.1");
        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.storeUnavailable()).isFalse();
        assertThat(fourth.verdict().blockedScope()).isEqualTo("APP");
    }

    @Test
    void storeError_failClosed_blocksWith503Verdict() {
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(false, 5, 50_000));

        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");
        assertThat(gate.allowed()).isFalse();
        assertThat(gate.storeUnavailable()).isTrue();
    }

    @Test
    void storeError_failClosed_twoLayers_oneVerdict() {
        // 两层都配了额度时，存储故障的决策对整笔请求生效：一起挡回，
        // 不存在「应用层放行、来源层另算」这类分裂路径
        quotaRepository.rows.add(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 10, null, null, null));
        quotaRepository.rows.add(RateQuota.reconstitute(RateLimitScope.IP, "app-1", "1.1.1.1", 2, null, null, null));
        catalog.refreshBlock(Duration.ofSeconds(5));
        windowStore.fail = true;
        RateLimiter rl = limiter(props(false, 5, 50_000));

        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");
        assertThat(gate.allowed()).isFalse();
        assertThat(gate.storeUnavailable()).isTrue();
    }

    @Test
    void storeTimeout_doesNotHang() {
        givenAppQuota(10);
        // 存储永不返回；限流器必须在 redisTimeout（100ms）内给出 fail-open 决策
        windowStore.hang = true;
        RateLimiter rl = limiter(props(true, 5, 50_000));

        Instant start = Instant.now();
        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");
        long elapsed = Duration.between(start, Instant.now()).toMillis();

        assertThat(gate.allowed()).isTrue();
        // 留足调度余量，但远小于「死等」级别（秒级），证明到点即决策
        assertThat(elapsed).isLessThan(800);
    }

    @Test
    void circuitOpens_afterThreshold_thenSkipsStore() {
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(true, 3, 60_000));

        for (int i = 0; i < 3; i++) {
            assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        }
        int callsAfterThreshold = windowStore.calls.get();
        assertThat(callsAfterThreshold).isEqualTo(3);

        // 熔断已打开：再来请求不再打存储
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(windowStore.calls.get()).isEqualTo(callsAfterThreshold);
    }

    @Test
    void halfOpen_allowsExactlyOneProbe_andReopensOnProbeFailure() throws Exception {
        givenAppQuota(10);
        windowStore.fail = true;
        // 2 次失败熔断，开窗期 200ms
        RateLimiter rl = limiter(props(true, 2, 200));

        check(rl, "app-1", "1.1.1.1");
        check(rl, "app-1", "1.1.1.1");
        assertThat(windowStore.calls.get()).isEqualTo(2);

        // 打开期间：多少笔都不再打存储
        for (int i = 0; i < 5; i++) {
            check(rl, "app-1", "1.1.1.1");
        }
        assertThat(windowStore.calls.get()).isEqualTo(2);

        // 等过开窗期，20 笔并发涌入：只有一笔探活真正打到存储，其余按熔断期策略放行
        Thread.sleep(250);
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger allowed = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    if (check(rl, "app-1", "1.1.1.1").allowed()) {
                        allowed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(windowStore.calls.get()).isEqualTo(3); // 恰好一笔探活
        assertThat(allowed.get()).isEqualTo(threads);     // fail-open：全部放行

        // 探活失败 → 重新计时熔断：随后一笔也不再打存储
        check(rl, "app-1", "1.1.1.1");
        assertThat(windowStore.calls.get()).isEqualTo(3);
    }

    @Test
    void circuitRecovers_afterOpenWindow_andSuccessfulProbe() {
        givenAppQuota(10);
        windowStore.fail = true;
        // 熔断打开时长设极短，下一笔请求即进入半开探活
        RateLimiter rl = limiter(props(true, 2, 1));

        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        int callsWhenOpen = windowStore.calls.get();

        // 等到熔断开窗期过去
        try {
            Thread.sleep(20);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        windowStore.fail = false;
        RateLimiter.Gate gate = check(rl, "app-1", "1.1.1.1");
        assertThat(gate.allowed()).isTrue();
        assertThat(gate.storeUnavailable()).isFalse();
        assertThat(windowStore.calls.get()).isGreaterThan(callsWhenOpen);

        // 探活成功后闭合，后续继续正常走存储
        assertThat(check(rl, "app-1", "1.1.1.1").storeUnavailable()).isFalse();
    }

    @Test
    void healthyStore_passesVerdictThrough() {
        givenAppQuota(2);
        RateLimiter rl = limiter(props(true, 5, 50_000));
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        assertThat(check(rl, "app-1", "1.1.1.1").allowed()).isTrue();
        RateLimiter.Gate third = check(rl, "app-1", "1.1.1.1");
        assertThat(third.allowed()).isFalse();
        assertThat(third.verdict().blockedScope()).isEqualTo("APP");
        assertThat(third.verdict().retryAfterSec()).isBetween(1L, 60L);
    }

    /** 内存里就能模拟计数的假存储（不依赖 Redis），支持失败/挂起两种故障。 */
    static class FakeWindowStore implements RateLimitWindowStore {
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean fail = false;
        volatile boolean hang = false;
        final java.util.Map<String, Integer> counters = new java.util.HashMap<>();
        Integer appLimit;
        Integer ipLimit;

        @Override
        public Mono<RateLimitVerdict> checkAndConsume(String appNo, String ip, Integer appL, Integer ipL) {
            calls.incrementAndGet();
            if (hang) {
                return Mono.never();
            }
            if (fail) {
                return Mono.error(new RuntimeException("redis down"));
            }
            this.appLimit = appL;
            this.ipLimit = ipL;
            synchronized (counters) {
                if (appL != null) {
                    int c = counters.getOrDefault(appNo + ":app", 0);
                    if (c >= appL) {
                        return Mono.just(RateLimitVerdict.reject("APP", 30));
                    }
                    counters.put(appNo + ":app", c + 1);
                }
                if (ipL != null) {
                    String k = appNo + ":ip:" + ip;
                    int c = counters.getOrDefault(k, 0);
                    if (c >= ipL) {
                        return Mono.just(RateLimitVerdict.reject("IP", 30));
                    }
                    counters.put(k, c + 1);
                }
            }
            return Mono.just(RateLimitVerdict.pass());
        }
    }

    static class FakeRepository implements RateLimitRepository {
        final List<RateQuota> rows = new ArrayList<>();

        @Override
        public RateQuota upsert(RateLimitScope scope, String appNo, String ip, Integer l, String by, long now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(RateLimitScope scope, String appNo, String ip) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<RateQuota> find(RateLimitScope scope, String appNo, String ip) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<RateQuota> loadAll() {
            return List.copyOf(rows);
        }
    }
}
