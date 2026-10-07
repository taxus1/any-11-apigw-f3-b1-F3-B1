package com.apigw.proxy.ratelimit;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 转发链路上的限流门面：读额度快照 → 调计数存储判定 → 处理存储故障。
 *
 * 两层都「不限」时直接放行，连 Redis 都不碰（默认也没配的应用零额外开销）。
 *
 * <b>计数归属只有一份（硬约束）</b>：任何时刻、任何路径上，计数都只发生在所有实例
 * 共享的同一份 Redis（单段 Lua 原子判定，见 rate-limit.lua）。本进程内没有任何形式的
 * 本机计数、本机兜底、本机小账本——曾经有过「存储抖动时交本机窗口兜底」，被刻意移除：
 * 那等于每台机器各算一份额度，N 台实例合计放出 N 倍；而且压力越大（Redis 越慢、
 * 越容易超时熔断）漏得越多，监控上看起来「还在限」，比明说的短暂放行更危险。
 * 所以故障路径上只有两种明说的行为：放行且不计数（fail-open），或挡回（fail-closed）。
 *
 * <b>计数存储暂时不可用时的完整决策矩阵</b>（五种情形，同一个策略开关）：
 * <ol>
 *   <li><b>调用超时</b>（{@code redisTimeout}，默认 100ms 到点）：绝不死等——到点立刻
 *       记一次连续失败并按策略决策，Redis 抖动不会把每个请求、每条 Netty 事件链
 *       拖成秒级等待而拖垮网关；</li>
 *   <li><b>连不上 / 脚本执行出错</b>：与超时同等对待——记一次连续失败、按策略决策；</li>
 *   <li><b>熔断打开期间</b>（连续失败达 {@code circuit-breaker-threshold} 后的
 *       {@code circuit-breaker-open-ms} 内）：根本不再发 Redis 请求，连 100ms 都不等，
 *       直接按策略决策——Redis 真挂时网关在限流上的开销约等于零；</li>
 *   <li><b>半开探活</b>：熔断开窗期一到，只放<b>一笔</b>真实请求去 Redis 探活
 *       （CAS 保证同一时刻只有一笔在探；其余并发请求仍按熔断期策略决策，不在探活
 *       瞬间打爆刚恢复的 Redis）。探活这一笔走的是全局口径：真计数、真判定，它的
 *       结果就是本笔的最终结果；探活成功熔断闭合、后续全部回到全局计数，探活失败
 *       重新计时熔断、下一个开窗期再探。</li>
 * </ol>
 *
 * <b>策略本身</b>（{@code apigw.rate-limit.fail-open-on-error}）：
 * <ul>
 *   <li>{@code true}（默认）fail-open：上述故障路径<b>放行且不补计数</b>——存储不可用，
 *       无处可计，也不本机另算。理由：限流是保护上游的「阀门」，阀门的动力源（Redis）
 *       断了时若默认全挡，等于让 Redis 这一个基础组件的故障变成全站调用失败、网关
 *       自己成单点；配合「短超时 + 立即熔断」，故障窗口内是短暂放行且已快速摘流，
 *       不会形成持续冲击。打 warn + 计数指标，运维必须告警。存储恢复后从下一笔起
 *       在 Redis 当前窗已有值上继续累加——不为故障期「补账」而误伤恢复后的正常请求。</li>
 *   <li>{@code false} fail-closed：故障路径一律挡回 503 RATE_LIMIT_STORE_UNAVAILABLE，
 *       不打上游。给「宁可短暂不可用也不裸放」的严格场景。</li>
 * </ul>
 *
 * 两条一致性口径：
 * <ul>
 *   <li><b>两层额度同路</b>：应用总量与来源地址在正常路径上由同一段 Lua 一次判完；
 *       故障路径上策略对整笔请求生效——不存在「一层按全局、一层按本机」的分裂路径；</li>
 *   <li><b>窗口与 Retry-After 同源</b>：窗口边界与重试等待秒数只有 Lua 用 Redis 服务端
 *       TIME 算出的那一份（fail-closed 的 503 不是窗口概念，不带 Retry-After）。</li>
 * </ul>
 */
@Slf4j
public class RateLimiter {

    private final RateLimitCatalog catalog;
    private final RateLimitWindowStore store;
    private final RateLimitProperties properties;

    /** 连续失败计数；达到阈值熔断打开。 */
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    /** 熔断打开的起始时刻（毫秒，0=未打开）。 */
    private final AtomicLong openedAtMs = new AtomicLong(0);
    /** 半开探活在飞标记：同一时刻只放一笔真实请求去探，其余按熔断期策略决策。 */
    private final AtomicBoolean probeInFlight = new AtomicBoolean(false);

    /** 存储故障的决策结果：fail-open 时 allowed=true、storeUnavailable=true（过滤器据此放行）。 */
    public record Gate(boolean allowed, boolean storeUnavailable,
                       RateLimitVerdict verdict) {
        static Gate pass() {
            return new Gate(true, false, RateLimitVerdict.pass());
        }
    }

    public RateLimiter(RateLimitCatalog catalog, RateLimitWindowStore store,
                       RateLimitProperties properties) {
        this.catalog = catalog;
        this.store = store;
        this.properties = properties;
    }

    public Mono<Gate> check(String appNo, String canonicalIp) {
        RateLimitCatalog.EffectiveQuota quota = catalog.resolve(appNo, canonicalIp);
        if (quota.nothingLimited()) {
            return Mono.just(Gate.pass());
        }

        if (!shouldCallStore()) {
            // 熔断打开（且没抢到探活名额）：不发 Redis，立即按策略决策（关键：不等待）
            return Mono.just(onUnavailable("circuit-open"));
        }

        return store.checkAndConsume(appNo, canonicalIp, quota.appPerMinute(), quota.ipPerMinute())
                .timeout(properties.redisTimeout())
                .map(verdict -> {
                    onStoreSuccess();
                    return new Gate(verdict.allowed(), false, verdict);
                })
                .onErrorResume(err -> Mono.just(onStoreFailure(err)));
    }

    /** 本笔是否真实调用计数存储：闭合期都放；打开期都不放；开窗期只放一笔探活。 */
    private boolean shouldCallStore() {
        long opened = openedAtMs.get();
        if (opened == 0) {
            return true;
        }
        if (System.currentTimeMillis() - opened < properties.circuitBreakerOpenMs()) {
            return false;
        }
        // 半开：CAS 保证同一时刻只有一笔去探活，其余并发请求仍按熔断期策略决策
        return probeInFlight.compareAndSet(false, true);
    }

    /** 一次真实调用成功：清失败计数、闭合熔断、收探活标记。 */
    private void onStoreSuccess() {
        consecutiveFailures.set(0);
        openedAtMs.set(0);
        probeInFlight.set(false);
    }

    /** 一次真实调用失败（超时/连不上/脚本错）：累计失败、（重新）打开熔断，再按策略决策。 */
    private Gate onStoreFailure(Throwable err) {
        int n = consecutiveFailures.incrementAndGet();
        if (openedAtMs.get() != 0) {
            // 探活失败：先重新计时熔断、再收探活标记——顺序不能反，先收标记会让别的
            // 线程看到「熔断到点且无探活在飞」再放进一笔，半开就成了多探
            openedAtMs.set(System.currentTimeMillis());
            log.warn("限流计数存储探活失败（{}），熔断再保持 {}ms",
                    err.toString(), properties.circuitBreakerOpenMs());
        } else if (n >= properties.circuitBreakerThreshold()
                && openedAtMs.compareAndSet(0, System.currentTimeMillis())) {
            log.warn("限流计数存储连续 {} 次失败（{}），熔断打开 {}ms，期间按 {} 处理",
                    n, err.toString(), properties.circuitBreakerOpenMs(),
                    properties.failOpenOnError() ? "放行(fail-open)" : "全挡(fail-closed)");
        } else {
            log.debug("限流计数存储失败（第 {} 次，{}）", n, err.toString());
        }
        probeInFlight.set(false);
        return onUnavailable(err.toString());
    }

    /**
     * 存储不可用时的策略决策：fail-open 放行且不计数；fail-closed 挡回。
     * 这里刻意没有任何本机计数——额度归属只有共享 Redis 一份，见类注释。
     */
    private Gate onUnavailable(String reason) {
        if (!properties.failOpenOnError()) {
            return new Gate(false, true, RateLimitVerdict.reject("STORE", 0));
        }
        log.debug("计数存储不可用（{}），按 fail-open 放行（不计数）", reason);
        return new Gate(true, true, RateLimitVerdict.pass());
    }
}
