package com.apigw.proxy.ratelimit;

import com.apigw.common.web.GatewayHeaders;
import com.apigw.domain.ratelimit.RateLimitRepository;
import com.apigw.domain.ratelimit.RateLimitScope;
import com.apigw.domain.ratelimit.RateQuota;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.WebHandler;
import org.springframework.web.server.adapter.HttpWebHandlerAdapter;
import org.springframework.web.server.handler.FilteringWebHandler;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 限流过滤器端到端（真实 Netty + 真实 HTTP 客户端，计数用内存假存储，无 Redis）。
 *
 * 覆盖：
 * - 额度内放行，超限 429，带 Retry-After（>=1 秒）与 X-Gateway-Error；
 * - 应用总量与来源地址两层：刷凶的地址只卡自己，同应用其他来源照放；
 * - 被限请求不打上游（tail 调用次数不增长）；
 * - 管理接口 /api 不参与限流；
 * - fail-closed 口径下计数存储不可用回 503；fail-open 放行。
 */
class RateLimitWebFilterTest {

    private RateLimitCatalog catalog;
    private MemoryWindowStore store;
    private RateLimitProperties properties;
    private FakeRepository repo;
    private AtomicInteger upstreamHits;
    private DisposableServer server;
    private String baseUrl;
    private WebClient client;

    record Resp(int status, String errorHeader, String retryAfter, String body) {
    }

    @BeforeEach
    void setUp() {
        repo = new FakeRepository();
        catalog = new RateLimitCatalog(repo);
        store = new MemoryWindowStore();
        upstreamHits = new AtomicInteger();
        properties = RateLimitProperties.defaults();

        RateLimiter limiter = new RateLimiter(catalog, store, properties);
        RateLimitWebFilter filter = new RateLimitWebFilter(limiter, new ObjectMapper());

        WebHandler tail = exchange -> {
            upstreamHits.incrementAndGet();
            byte[] body = ("OK app=" + exchange.getRequest().getHeaders().getFirst(GatewayHeaders.APP_NO_HEADER))
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            return exchange.getResponse().writeWith(
                    Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
        };
        WebHandler filtering = new FilteringWebHandler(tail, List.of(filter));
        HttpWebHandlerAdapter adapter = new HttpWebHandlerAdapter(filtering);
        adapter.afterPropertiesSet();

        server = HttpServer.create().handle(new ReactorHttpHandlerAdapter((HttpHandler) adapter)).bindNow();
        baseUrl = "http://127.0.0.1:" + server.port();
        client = WebClient.builder().build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.disposeNow();
        }
    }

    private void loadQuota(RateQuota... quotas) {
        repo.rows.clear();
        repo.rows.addAll(List.of(quotas));
        catalog.refreshBlock(Duration.ofSeconds(5));
    }

    private Resp call(String path, String appNo, String xff) {
        return client.get().uri(baseUrl + path)
                .headers(h -> {
                    if (appNo != null) {
                        h.set(GatewayHeaders.APP_NO_HEADER, appNo);
                    }
                    if (xff != null) {
                        h.set(com.apigw.common.web.ClientIpResolver.XFF_HEADER, xff);
                    }
                })
                .exchangeToMono(resp -> resp.bodyToMono(String.class).defaultIfEmpty("").map(b -> new Resp(
                        resp.statusCode().value(),
                        resp.headers().asHttpHeaders().getFirst("X-Gateway-Error"),
                        resp.headers().asHttpHeaders().getFirst("Retry-After"),
                        b)))
                .block(Duration.ofSeconds(5));
    }

    @Test
    void noQuota_configuredOrDefault_allPass() {
        loadQuota(); // 一行额度都没有
        for (int i = 0; i < 20; i++) {
            assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        }
        assertThat(upstreamHits.get()).isEqualTo(20);
        assertThat(store.calls.get()).isZero(); // 两层都不限：不碰计数存储
    }

    @Test
    void appQuota_allowsExactlyLimit_then429_withRetryAfter_andNoUpstream() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 3, null, null, null));

        assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "2.2.2.2").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "3.3.3.3").status()).isEqualTo(200);

        Resp fourth = call("/order/1", "app-1", "4.4.4.4");
        assertThat(fourth.status()).isEqualTo(429);
        assertThat(fourth.errorHeader()).isEqualTo("RATE_LIMITED_APP");
        // 等待时长口径：当前固定窗口结束还要等的整秒数，1..60，且响应体是网关 JSON
        assertThat(Integer.parseInt(fourth.retryAfter())).isBetween(1, 60);
        assertThat(fourth.body()).contains("RATE_LIMITED_APP");
        // 关键：第 4 笔被挡，没有打上游（上游只收到 3 笔）
        assertThat(upstreamHits.get()).isEqualTo(3);
    }

    @Test
    void ipQuota_blocksOnlyNoisySource_otherSourcesStillPass() {
        loadQuota(
                RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 100, null, null, null),
                RateQuota.reconstitute(RateLimitScope.IP, "app-1", "9.9.9.9", 2, null, null, null));

        // 刷凶的地址：第 3 笔被来源层卡住
        assertThat(call("/order/1", "app-1", "9.9.9.9").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "9.9.9.9").status()).isEqualTo(200);
        Resp blocked = call("/order/1", "app-1", "9.9.9.9");
        assertThat(blocked.status()).isEqualTo(429);
        assertThat(blocked.errorHeader()).isEqualTo("RATE_LIMITED_IP");
        assertThat(Integer.parseInt(blocked.retryAfter())).isBetween(1, 60);

        // 同应用的正常来源不受连累（应用总量 100 远没用完）
        assertThat(call("/order/1", "app-1", "8.8.8.8").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "7.7.7.7").status()).isEqualTo(200);
    }

    @Test
    void defaultQuota_appliesToAppsWithoutOwnConfig() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.DEFAULT, "*", null, 2, null, null, null));

        assertThat(call("/order/1", "whatever-app", "1.1.1.1").status()).isEqualTo(200);
        assertThat(call("/order/1", "whatever-app", "1.1.1.1").status()).isEqualTo(200);
        Resp r = call("/order/1", "whatever-app", "1.1.1.1");
        assertThat(r.status()).isEqualTo(429);
        assertThat(r.errorHeader()).isEqualTo("RATE_LIMITED_APP");
    }

    @Test
    void appExplicitlyUnlimited_ignoresDefault() {
        loadQuota(
                RateQuota.reconstitute(RateLimitScope.DEFAULT, "*", null, 1, null, null, null),
                RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, null, null, null, null));
        for (int i = 0; i < 5; i++) {
            assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        }
    }

    @Test
    void managementApi_notRateLimited() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 1, null, null, null));
        assertThat(call("/api/gateway/rate-limits/apps/app-1", "app-1", null).status()).isEqualTo(200);
        assertThat(call("/api/gateway/rate-limits/apps/app-1", "app-1", null).status()).isEqualTo(200);
    }

    @Test
    void noAppHeader_passesThrough() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.DEFAULT, "*", null, 1, null, null, null));
        assertThat(call("/order/1", null, null).status()).isEqualTo(200);
        assertThat(store.calls.get()).isZero();
    }

    @Test
    void storeUnavailable_failClosed_returns503_andNoUpstream() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 5, null, null, null));
        store.fail = true;
        RateLimitProperties failClosed = new RateLimitProperties(true, Duration.ofSeconds(10), 10_000,
                Duration.ofMillis(100), false, 1, 60_000, 60);
        RateLimiter limiter = new RateLimiter(catalog, store, failClosed);

        installSingleFilter(limiter);
        Resp r = call("/order/1", "app-1", "1.1.1.1");
        assertThat(r.status()).isEqualTo(503);
        assertThat(r.errorHeader()).isEqualTo("RATE_LIMIT_STORE_UNAVAILABLE");
        assertThat(upstreamHits.get()).isZero();
    }

    @Test
    void storeUnavailable_failOpen_passes_andQuotaUntouchedAfterRecovery() {
        loadQuota(RateQuota.reconstitute(RateLimitScope.APP, "app-1", null, 3, null, null, null));
        store.fail = true;
        // 阈值调高不熔断，让每笔都走「调用失败 → fail-open」；fail-open 是放行且不计数，
        // 没有任何本机小账本——否则多台网关各算一份，全局额度就被实例数放大
        RateLimitProperties failOpenNoCircuit = new RateLimitProperties(true, Duration.ofSeconds(10), 10_000,
                Duration.ofMillis(100), true, 1_000, 60_000, 60);
        installSingleFilter(new RateLimiter(catalog, store, failOpenNoCircuit));

        // 计数存储挂了：fail-open 放行，上游照收（不少放、也不暗地按本机另算）
        for (int i = 0; i < 5; i++) {
            assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        }
        assertThat(upstreamHits.get()).isEqualTo(5);

        // 存储恢复：故障期放行的 5 笔没消耗任何名额，当前窗仍精确放 3 笔、第 4 笔 429
        store.fail = false;
        assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        assertThat(call("/order/1", "app-1", "1.1.1.1").status()).isEqualTo(200);
        Resp fourth = call("/order/1", "app-1", "1.1.1.1");
        assertThat(fourth.status()).isEqualTo(429);
        assertThat(fourth.errorHeader()).isEqualTo("RATE_LIMITED_APP");
        assertThat(Integer.parseInt(fourth.retryAfter())).isBetween(1, 60);
        assertThat(upstreamHits.get()).isEqualTo(8);
    }

    /** 故障策略随用例而变：用新的 limiter 重建一次过滤器链。 */
    private void installSingleFilter(RateLimiter limiter) {
        server.disposeNow();
        RateLimitWebFilter filter = new RateLimitWebFilter(limiter, new ObjectMapper());
        WebHandler tail = exchange -> {
            upstreamHits.incrementAndGet();
            byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            return exchange.getResponse().writeWith(
                    Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
        };
        HttpWebHandlerAdapter adapter = new HttpWebHandlerAdapter(new FilteringWebHandler(tail, List.of(filter)));
        adapter.afterPropertiesSet();
        server = HttpServer.create().handle(new ReactorHttpHandlerAdapter((HttpHandler) adapter)).bindNow();
        baseUrl = "http://127.0.0.1:" + server.port();
        client = WebClient.builder().build();
    }

    /** 内存计数假存储：与 Lua 同语义（先查后加、拒绝不加），供过滤器端到端用。 */
    static class MemoryWindowStore implements RateLimitWindowStore {
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean fail = false;
        final java.util.Map<String, Integer> counters = new java.util.HashMap<>();

        @Override
        public Mono<RateLimitVerdict> checkAndConsume(String appNo, String ip, Integer appL, Integer ipL) {
            calls.incrementAndGet();
            if (fail) {
                return Mono.error(new RuntimeException("redis down"));
            }
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
