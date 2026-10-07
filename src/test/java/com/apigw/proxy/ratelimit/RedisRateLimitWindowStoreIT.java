package com.apigw.proxy.ratelimit;

import com.apigw.support.EnabledIfRedis;
import com.apigw.support.RedisAvailableCondition;
import io.lettuce.core.resource.ClientResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis 固定窗口计数的真机集成测试（连真实 Redis；不可达时整类跳过）。
 *
 * 用短窗口（2s）加速窗口轮转，验证：
 * - 精确放行到额度、第 N+1 笔拒绝且 Retry-After 在窗口剩余范围内；
 * - 拒绝不占名额（被 IP 层挡住的请求不消耗应用总量）；
 * - 窗口到点从零重新计数，不带余数；
 * - 多线程并发在同一窗口抢名额：全局计数下「放行数恰等于额度」，不超发也不少放；
 * - 计数键带 TTL，窗口结束后自动过期被回收（存储不无限膨胀）。
 */
@EnabledIfRedis
class RedisRateLimitWindowStoreIT {

    private ReactiveRedisConnectionFactory factory;
    private ReactiveStringRedisTemplate redis;
    /** 测试用 2 秒窗口。 */
    private RedisRateLimitWindowStore store;
    private String app;

    @BeforeEach
    void setUp() {
        var clientConfig = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.create()).build();
        var connConfig = new RedisStandaloneConfiguration(
                RedisAvailableCondition.host(), RedisAvailableCondition.port());
        var lettuceFactory = new LettuceConnectionFactory(connConfig, clientConfig);
        lettuceFactory.afterPropertiesSet();
        this.factory = lettuceFactory;
        this.redis = new ReactiveStringRedisTemplate(lettuceFactory);
        this.store = new RedisRateLimitWindowStore(redis, 2);
        this.app = "it-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void tearDown() {
        // 清掉本应用 hash tag 下的所有键
        var keys = redis.keys("apigw:rl:{" + app + "}*").collectList().block();
        if (keys != null && !keys.isEmpty()) {
            redis.delete(Flux.fromIterable(keys)).block();
        }
        ((LettuceConnectionFactory) factory).destroy();
    }

    private RateLimitVerdict check(String ip, Integer appL, Integer ipL) {
        return store.checkAndConsume(app, ip, appL, ipL).block(Duration.ofSeconds(3));
    }

    @Test
    void allowsExactlyAppLimit_thenRejectsWithRetryAfter() {
        RateLimitVerdict v1 = check("1.1.1.1", 3, null);
        RateLimitVerdict v2 = check("2.2.2.2", 3, null);
        RateLimitVerdict v3 = check("3.3.3.3", 3, null);
        assertThat(List.of(v1.allowed(), v2.allowed(), v3.allowed())).containsOnly(true);

        RateLimitVerdict rejected = check("4.4.4.4", 3, null);
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.blockedScope()).isEqualTo("APP");
        // 2s 窗口内等待秒数只能是 1 或 2（向上取整）
        assertThat(rejected.retryAfterSec()).isBetween(1L, 2L);
    }

    @Test
    void ipLayer_isIndependent_andRejectedDoesNotConsumeAppQuota() {
        // 应用总量 100，坏地址额度 2
        assertThat(check("9.9.9.9", 100, 2).allowed()).isTrue();
        assertThat(check("9.9.9.9", 100, 2).allowed()).isTrue();
        RateLimitVerdict blocked = check("9.9.9.9", 100, 2);
        assertThat(blocked.allowed()).isFalse();
        assertThat(blocked.blockedScope()).isEqualTo("IP");

        // 坏地址再刷多次全被挡；同应用其他地址照常，证明被挡请求没吃应用总量
        for (int i = 0; i < 10; i++) {
            assertThat(check("9.9.9.9", 100, 2).allowed()).isFalse();
        }
        for (int i = 0; i < 5; i++) {
            assertThat(check("8.8.8." + i, 100, null).allowed()).isTrue();
        }
    }

    @Test
    void windowResetsToZero_afterBoundary() throws InterruptedException {
        RateLimitVerdict first = check("1.1.1.1", 1, null);
        assertThat(first.allowed()).isTrue();
        assertThat(check("1.1.1.1", 1, null).allowed()).isFalse();

        // 等过窗口边界（2s 窗口 + 宽限余量）
        Thread.sleep(2_300);

        // 新窗口从 0 开始：上一窗余数不带过来
        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
        assertThat(check("1.1.1.1", 1, null).allowed()).isFalse();
    }

    @Test
    void concurrentBurst_neverOverAdmits() throws InterruptedException {
        int limit = 200;
        int threads = 16;
        int perThread = 40; // 合计 640 抢 200
        // 本用例的不变量「放行恰等于额度」只在同一个窗口内成立（固定窗口每窗各放一份额度）。
        // 用独立的 60s 长窗存储，并避开窗口边界再齐射，防止突发流量跨窗把两窗的额度算进一次断言
        RedisRateLimitWindowStore longWindowStore = new RedisRateLimitWindowStore(redis, 60);
        long now = System.currentTimeMillis();
        long intoWindow = now % 60_000L;
        if (intoWindow > 55_000L) {
            Thread.sleep(60_000L - intoWindow + 1_000L);
        }
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger allowed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final String ip = "10.0.0." + t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        RateLimitVerdict v = longWindowStore
                                .checkAndConsume(app, ip, limit, null)
                                .block(Duration.ofSeconds(3));
                        if (v.allowed()) {
                            allowed.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        // 硬性不变量：全局共享计数下，16 线程合计放行恰好 = 额度，不多也不少
        assertThat(allowed.get()).isEqualTo(limit);
        assertThat(rejected.get()).isEqualTo(threads * perThread - limit);

        // Redis 里该窗口键的终值也必须正好停在额度上
        var keyNames = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(keyNames).hasSize(1);
        Long finalCount = redis.opsForValue().get(keyNames.get(0)).map(Long::parseLong).block();
        assertThat(finalCount).isEqualTo(limit);
    }

    @Test
    void windowKeys_expireByTtl() throws InterruptedException {
        assertThat(check("1.1.1.1", 1, null).allowed()).isTrue();
        var before = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(before).hasSize(1);
        Duration ttl = redis.getExpire(before.get(0)).block();
        // TTL = 窗口 2s + 30s 宽限；放宽断言在 2s..35s 之间
        assertThat(ttl).isNotNull();
        assertThat(ttl.getSeconds()).isBetween(2L, 35L);

        // 等过 TTL（2s 窗口 + 30s 宽限，再留余量）
        Thread.sleep(33_000);
        var after = redis.keys("apigw:rl:{" + app + "}:app:*").collectList().block();
        assertThat(after == null || after.isEmpty()).isTrue();
    }

    @Test
    void bothLayersUnlimited_doesNotCreateKeys() {
        for (int i = 0; i < 5; i++) {
            RateLimitVerdict v = check("1.1.1.1", null, null);
            assertThat(v.allowed()).isTrue();
        }
        var keys = redis.keys("apigw:rl:{" + app + "}*").collectList().block();
        // 两层都不限时脚本不 INCR；锚点键也不应被创建（从未写入）
        List<String> concrete = new ArrayList<>();
        if (keys != null) {
            concrete.addAll(keys);
        }
        assertThat(concrete).isEmpty();
    }
}
