package com.apigw.proxy.ratelimit;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 转发链路上的限流门面：读额度快照 → 调计数存储判定 → 处理存储故障。
 *
 * 两层都「不限」时直接放行，连 Redis 都不碰（默认也没配的应用零额外开销）。
 *
 * 计数存储暂时不可用（超时/连不上/执行出错）时的策略，先定死如下（理由很重要）：
 * <ol>
 *   <li><b>绝不能把请求卡死在计数器上</b>——这是比「放不放行」更高优先级的约束。
 *       所以每次计数调用都套 {@code redisTimeout}（默认 100ms），到点立刻结束，
 *       Redis 抖动不会把每个请求、每条 Netty 事件链都拖成秒级等待而拖垮网关；</li>
 *   <li>再加一道熔断器：连续失败 N 次（默认 5）后熔断打开，打开期间（默认 5s）
 *       <b>根本不再发 Redis 请求</b>，直接按既定策略决策，连 100ms 都不等；
 *       到点放一笔半开探活，成功即闭合。Redis 真挂了时网关开销约等于零；</li>
 *   <li>熔断/超时期间放行还是挡回，由 {@code apigw.rate-limit.fail-open-on-error}
 *       决定，<b>默认 fail-open（放行）</b>：限流是保护上游的「阀门」，阀门的动力源
 *       （Redis）断了时，若默认全挡，等于让 Redis 这一个基础组件的故障变成全站调用失败，
 *       网关自己成了单点。配合「短超时 + 立即熔断」，Redis 故障窗口内短暂放行、且快速
 *       摘流，不会形成持续冲击；同时打 warn + 计数指标，运维必须告警。
 *       对宁可短暂不可用也不接受裸放的严格场景，置 {@code fail-open-on-error=false}
 *       即 fail-closed（回 503 RATE_LIMIT_STORE_UNAVAILABLE，由过滤器统一答复）。</li>
 * </ol>
 *
 * 注意 fail-open 放行的请求<b>不补计数</b>（存储都不可用，也无处可补）；存储恢复后
 * 熔断闭合，从下一笔起重新按当前窗口键计数——当前窗口可能已在 Redis 里有部分计数，
 * 直接在现有值上继续累加（不会为了「补」故障期间的量而误伤恢复后的正常请求）。
 */
@Slf4j
public class RateLimiter {

    private final RateLimitCatalog catalog;
    private final RateLimitWindowStore store;
    private final RateLimitProperties properties;
    /** 计数存储抖动时的本机兜底：这一段按本机窗口把额度挡住。 */
    private final LocalRateLimitWindowStore localFallback;

    /** 连续失败计数；达到阈值熔断打开。 */
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    /** 熔断打开的起始时刻（毫秒，0=未打开）。 */
    private final AtomicLong openedAtMs = new AtomicLong(0);

    /** 存储故障的决策结果：fail-open 时 allowed=true、storeUnavailable=true（过滤器据此放行）。 */
    public record Gate(boolean allowed, boolean storeUnavailable,
                       RateLimitVerdict verdict) {
        static Gate pass() {
            return new Gate(true, false, RateLimitVerdict.pass());
        }
    }

    public RateLimiter(RateLimitCatalog catalog, RateLimitWindowStore store,
                       RateLimitProperties properties) {
        this(catalog, store, properties,
                new LocalRateLimitWindowStore(properties.windowSeconds()));
    }

    public RateLimiter(RateLimitCatalog catalog, RateLimitWindowStore store,
                       RateLimitProperties properties, LocalRateLimitWindowStore localFallback) {
        this.catalog = catalog;
        this.store = store;
        this.properties = properties;
        this.localFallback = localFallback;
    }

    public Mono<Gate> check(String appNo, String canonicalIp) {
        RateLimitCatalog.EffectiveQuota quota = catalog.resolve(appNo, canonicalIp);
        if (quota.nothingLimited()) {
            return Mono.just(Gate.pass());
        }

        if (circuitOpen()) {
            // 熔断打开：不发 Redis，立即按策略决策（关键：不等待）
            return Mono.just(onUnavailable("circuit-open", appNo, canonicalIp, quota));
        }

        return store.checkAndConsume(appNo, canonicalIp, quota.appPerMinute(), quota.ipPerMinute())
                .timeout(properties.redisTimeout())
                .map(verdict -> {
                    consecutiveFailures.set(0);
                    return new Gate(verdict.allowed(), false, verdict);
                })
                .onErrorResume(err -> Mono.just(onFailure(err, appNo, canonicalIp, quota)));
    }

    /** 一次真实调用失败（超时/连不上/脚本错）：累计失败、必要时熔断，再按策略决策。 */
    private Gate onFailure(Throwable err, String appNo, String canonicalIp,
                           RateLimitCatalog.EffectiveQuota quota) {
        int n = consecutiveFailures.incrementAndGet();
        if (n >= properties.circuitBreakerThreshold() && openedAtMs.compareAndSet(0, System.currentTimeMillis())) {
            log.warn("限流计数存储连续 {} 次失败（{}），熔断打开 {}ms，期间按 {} 处理",
                    n, err.toString(), properties.circuitBreakerOpenMs(),
                    properties.failOpenOnError() ? "放行(fail-open)" : "全挡(fail-closed)");
        } else {
            log.debug("限流计数存储失败（第 {} 次，{}）", n, err.toString());
        }
        return onUnavailable(err.toString(), appNo, canonicalIp, quota);
    }

    private Gate onUnavailable(String reason, String appNo, String canonicalIp,
                               RateLimitCatalog.EffectiveQuota quota) {
        if (!properties.failOpenOnError()) {
            return new Gate(false, true, RateLimitVerdict.reject("STORE", 0));
        }
        // 存储抖动的这一小会儿不裸放行：交本机窗口兜底，抖动期间额度也挡在本机
        RateLimitVerdict local = localFallback.consume(appNo, canonicalIp,
                quota.appPerMinute(), quota.ipPerMinute());
        log.debug("计数存储不可用（{}），本机窗口兜底：allowed={} scope={} app={} ip={}",
                reason, local.allowed(), local.blockedScope(), appNo, canonicalIp);
        return new Gate(local.allowed(), true, local);
    }

    /** 熔断是否处于打开态；到点不在这里急着清，靠下一笔真实调用充当半开探活。 */
    private boolean circuitOpen() {
        long opened = openedAtMs.get();
        if (opened == 0) {
            return false;
        }
        if (System.currentTimeMillis() - opened >= properties.circuitBreakerOpenMs()) {
            // 半开：允许这一笔真实调用探活（成功会清失败计数）。CAS 只放一笔出去探，
            // 其余并发请求在本周期内仍按熔断处理，避免探活瞬间打爆刚恢复的 Redis
            if (openedAtMs.compareAndSet(opened, 0)) {
                consecutiveFailures.set(0);
                return false;
            }
            return true;
        }
        return true;
    }
}
