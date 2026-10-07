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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 限流器对「计数存储故障」的处理（不依赖 Redis，用假存储）：
 * - 两层都不限：根本不调存储；
 * - 存储超时/出错：短超时后按 fail-open（默认放行）或 fail-closed（挡回）决策，不死等；
 * - 连续失败达阈值熔断打开：打开期间不再调存储（调用计数不再增长），立即决策；
 * - 恢复（探活成功）后闭合，重新正常计数；
 * - <b>额度只认共享存储一份</b>：故障期间三台限流器都不得按本机另算额度（不出现放大三倍），
 *   fail-open 放行的请求不补计数；半开探活拿到的真实「拒绝」也被如实采纳。
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

    @Test
    void nothingLimited_doesNotTouchStore() {
        // 不给 app-1 配任何额度，也没默认 → 两层都不限
        catalog.refreshBlock(Duration.ofSeconds(5));
        RateLimiter rl = limiter(props(true, 5, 5_000));
        RateLimiter.Gate gate = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));

        assertThat(gate.allowed()).isTrue();
        assertThat(windowStore.calls.get()).isZero();
    }

    @Test
    void storeError_defaultFailOpen_allowsAndFlagsUnavailable() {
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(true, 5, 50_000));

        RateLimiter.Gate gate = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(gate.allowed()).isTrue();
        assertThat(gate.storeUnavailable()).isTrue();
    }

    @Test
    void storeError_failClosed_blocksWith503Verdict() {
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(false, 5, 50_000));

        RateLimiter.Gate gate = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
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
        RateLimiter.Gate gate = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
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
            assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        }
        int callsAfterThreshold = windowStore.calls.get();
        assertThat(callsAfterThreshold).isEqualTo(3);

        // 熔断已打开：再来请求不再打存储
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(windowStore.calls.get()).isEqualTo(callsAfterThreshold);
    }

    @Test
    void circuitRecovers_afterOpenWindow_andSuccessfulProbe() {
        givenAppQuota(10);
        windowStore.fail = true;
        // 熔断打开时长设极短，下一笔请求即进入半开探活
        RateLimiter rl = limiter(props(true, 2, 1));

        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        int callsWhenOpen = windowStore.calls.get();

        // 等到熔断开窗期过去
        try {
            Thread.sleep(20);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        windowStore.fail = false;
        RateLimiter.Gate gate = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(gate.allowed()).isTrue();
        assertThat(gate.storeUnavailable()).isFalse();
        assertThat(windowStore.calls.get()).isGreaterThan(callsWhenOpen);

        // 探活成功后闭合，后续继续正常走存储
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).storeUnavailable()).isFalse();
    }

    @Test
    void healthyStore_passesVerdictThrough() {
        givenAppQuota(2);
        RateLimiter rl = limiter(props(true, 5, 50_000));
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        RateLimiter.Gate third = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(third.allowed()).isFalse();
        assertThat(third.verdict().blockedScope()).isEqualTo("APP");
        assertThat(third.verdict().retryAfterSec()).isBetween(1L, 60L);
    }

    @Test
    void storeOutage_failOpen_threeInstancesEachHaveNoLocalQuota() {
        // 核心回归：存储挂掉时三台网关各自都不得「按本机另算一份」。
        // 三个相互独立的限流器（模拟三台机器），背后存储一直报错。
        // fail-open 下三台都必须无条件放行故障期间的每一笔（哪怕远超 100 额度），
        // 任何一台都不能用本机计数在第 101 笔开始挡——否则额度就被放大/割裂成三份。
        givenAppQuota(100);
        FakeWindowStore sharedDownStore = new FakeWindowStore();
        sharedDownStore.fail = true;
        RateLimiter rl1 = new RateLimiter(catalog, sharedDownStore, props(true, 5, 50_000));
        RateLimiter rl2 = new RateLimiter(catalog, sharedDownStore, props(true, 5, 50_000));
        RateLimiter rl3 = new RateLimiter(catalog, sharedDownStore, props(true, 5, 50_000));

        for (int i = 0; i < 250; i++) { // 每台打 250，远超 100
            for (RateLimiter rl : List.of(rl1, rl2, rl3)) {
                RateLimiter.Gate gate = rl.check("app-1", "1.1.1." + (i % 250 + 1))
                        .block(Duration.ofSeconds(2));
                assertThat(gate.allowed())
                        .as("存储故障 fail-open：不得出现本机额度，每一笔都按故障策略放行")
                        .isTrue();
                assertThat(gate.storeUnavailable()).isTrue();
            }
        }
    }

    @Test
    void storeOutage_failOpen_doesNotConsumeAnyCount() {
        // fail-open 放行的请求不补计数：故障期间没有任何一笔落到存储，恢复后在 Redis
        // 当前窗已有值上继续，而不是被「故障期间的本机计数」顶到上限。
        // 熔断开窗设极短，便于下面恢复后半开探活立刻闭合。
        givenAppQuota(3);
        RateLimiter rl = limiter(props(true, 5, 1));
        windowStore.fail = true;
        for (int i = 0; i < 10; i++) {
            assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        }
        // 故障存储只被尝试调用（会失败），没有一次成功计数
        assertThat(windowStore.calls.get()).isGreaterThan(0);
        assertThat(windowStore.counters).isEmpty();

        try {
            Thread.sleep(20);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        // 恢复：额度从 0 开始照常精确放到 3
        windowStore.fail = false;
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();
        RateLimiter.Gate fourth = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.storeUnavailable()).isFalse();
    }

    @Test
    void circuitOpen_failClosed_blocksEveryRequestWithoutStore() {
        // fail-closed + 熔断打开：连存储都不调，每一笔都挡回（storeUnavailable），
        // 不会因熔断而在本机放一部分。
        givenAppQuota(10);
        windowStore.fail = true;
        RateLimiter rl = limiter(props(false, 2, 60_000));

        for (int i = 0; i < 2; i++) {
            RateLimiter.Gate g = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
            assertThat(g.allowed()).isFalse();
            assertThat(g.storeUnavailable()).isTrue();
        }
        int calls = windowStore.calls.get();
        for (int i = 0; i < 5; i++) {
            RateLimiter.Gate g = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
            assertThat(g.allowed()).isFalse();
            assertThat(g.storeUnavailable()).isTrue();
        }
        // 熔断打开后不再发 Redis
        assertThat(windowStore.calls.get()).isEqualTo(calls);
    }

    @Test
    void halfOpenProbeThatGetsRejected_closesAndHonorsGlobalVerdict() {
        // 半开探活那一笔是真实计数调用：若存储此时已恢复且额度已满，探活结果是「拒绝」，
        // 这个拒绝必须被如实采纳（allowed=false、storeUnavailable=false），并视为探活成功而闭合。
        givenAppQuota(1);
        RateLimiter rl = limiter(props(true, 1, 1));

        // 健康路径先占掉唯一额度
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).allowed()).isTrue();

        // 存储打挂，下一笔即触发熔断（阈值 1），按 fail-open 放行
        windowStore.fail = true;
        assertThat(rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2)).storeUnavailable()).isTrue();

        try {
            Thread.sleep(20);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        windowStore.fail = false; // Redis 恢复，但额度 1 已被占满
        RateLimiter.Gate probe = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(probe.allowed()).isFalse();
        assertThat(probe.storeUnavailable()).isFalse();
        assertThat(probe.verdict().blockedScope()).isEqualTo("APP");

        // 探活（哪怕结果是业务上的限流拒绝）即视为存储恢复、熔断闭合：后续仍走真实存储
        RateLimiter.Gate next = rl.check("app-1", "1.1.1.1").block(Duration.ofSeconds(2));
        assertThat(next.allowed()).isFalse();
        assertThat(next.storeUnavailable()).isFalse();
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
