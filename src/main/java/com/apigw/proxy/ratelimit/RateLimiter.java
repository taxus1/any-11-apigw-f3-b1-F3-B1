package com.apigw.proxy.ratelimit;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 转发链路上的限流门面：读额度快照 → 调计数存储判定 → 处理存储故障。
 *
 * <b>额度归属只有一份（最高优先级约束）</b>：应用总量也好、来源地址也好，计数只存在于
 * 所有实例共享的同一份存储（Redis），由 {@link RateLimitWindowStore} 的单段 Lua 原子判定。
 * 本类<b>不持有任何进程内计数</b>——故障期间绝不退化成「按本机窗口各算一份」，否则 N 台网关
 * 会把同一额度放大成 N 倍（压力低时看不出来，一打满存储超时就漏量）。任何时刻、任何路径上，
 * 某一台都不得按本机另算一份量。
 *
 * 两层额度（应用总量 / 来源地址）在健康路径与所有故障路径上<b>同口径</b>：要么一起由共享
 * 存储判定，要么存储不可用时按同一策略整体放行/整体挡回，绝不存在「应用层按全局、来源层按
 * 本机」这种割裂。
 *
 * 计数存储暂时不可用（超时/连不上/执行出错/熔断打开/半开探活）时的策略，定死如下：
 * <ol>
 *   <li><b>绝不能把请求卡死在计数器上</b>——这是和「放不放行」并列的硬约束。
 *       每次计数调用都套 {@code redisTimeout}（默认 100ms），到点立刻决策，绝不死等，
 *       Redis 抖动不会拖垮 Netty 事件循环、拖垮整个网关；</li>
 *   <li>再加一道熔断器：连续失败 N 次（默认 5）后熔断打开，打开期间（默认 5s）
 *       <b>根本不再发 Redis 请求</b>，直接按既定策略决策，连 100ms 都不等；
 *       到点用 CAS 只放一笔半开探活，其余并发请求仍按熔断态处理，避免探活瞬间打爆刚恢复的
 *       Redis。探活成功即闭合（清失败计数），探活失败重新计熔断；</li>
 *   <li>熔断/超时期间放行还是挡回，由 {@code apigw.rate-limit.fail-open-on-error} 决定，
 *       <b>默认 fail-open（放行）</b>：限流是保护上游的「阀门」，阀门的动力源（Redis）断了时
 *       若默认全挡，等于让 Redis 一个基础组件故障变成全站调用失败、网关自己成单点。配合
 *       「短超时 + 立即熔断」，故障窗口内是<b>短暂</b>放行且已快速摘流，不会持续冲击；
 *       同时打 warn + 计数指标，运维必须告警。对「宁可短暂不可用也不裸放」的严格场景，置
 *       {@code fail-open-on-error=false} 即 fail-closed，回 503 RATE_LIMIT_STORE_UNAVAILABLE，
 *       同样不打上游。</li>
 * </ol>
 *
 * 各故障情形的统一处置（两层口径一致，不区分应用层/来源层）：
 * <ul>
 *   <li><b>等超时 / 连不上 / 执行出错</b>：记一次连续失败（达到阈值即熔断），然后按
 *       fail-open 放行或 fail-closed 挡回。这一笔没在共享存储里占名额，<b>也不在本机补算</b>；</li>
 *   <li><b>熔断打开</b>：不发 Redis，直接按同一策略放行/挡回（连 100ms 都不等）；</li>
 *   <li><b>半开探活</b>：放出去的那一笔是真实计数调用——成功就按存储返回的真实判定走
 *       （放行/拒绝都算数）并闭合；失败则按上面的出错情形处理并重整熔断，其余并发请求仍按
 *       熔断态的策略走。</li>
 * </ul>
 *
 * 注意 fail-open 放行的请求<b>不补计数</b>：既然额度只认共享存储这一份，就没有第二处可补；
 * 存储恢复、熔断闭合后，从下一笔起在当前窗 Redis 已有的值上继续累加，不为了「补」故障期间的
 * 量而误伤恢复后的正常请求。
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

    /**
     * 一次限流决策。
     *
     * @param allowed          最终是否放行
     * @param storeUnavailable 本次结论是不是在「计数存储不可用」下按故障策略得出的
     *                         （fail-open 时 allowed=true，fail-closed 时 allowed=false；
     *                         过滤器据此决定 fail-closed 要回 503）
     * @param verdict          健康路径上来自共享存储的真实判定；故障放行时是一个不带任何计数的
     *                         pass（没有本机计数结论冒充全局结论）
     */
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

        if (circuitOpen()) {
            // 熔断打开：不发 Redis，立即按策略决策（关键：不等待、不按本机另算）
            return Mono.just(onUnavailable("circuit-open", appNo, canonicalIp));
        }

        return store.checkAndConsume(appNo, canonicalIp, quota.appPerMinute(), quota.ipPerMinute())
                .timeout(properties.redisTimeout())
                .map(verdict -> {
                    consecutiveFailures.set(0);
                    return new Gate(verdict.allowed(), false, verdict);
                })
                .onErrorResume(err -> Mono.just(onFailure(err, appNo, canonicalIp)));
    }

    /** 一次真实调用失败（超时/连不上/脚本错，含半开探活失败）：累计失败、必要时熔断，再按策略决策。 */
    private Gate onFailure(Throwable err, String appNo, String canonicalIp) {
        int n = consecutiveFailures.incrementAndGet();
        if (n >= properties.circuitBreakerThreshold() && openedAtMs.compareAndSet(0, System.currentTimeMillis())) {
            log.warn("限流计数存储连续 {} 次失败（{}），熔断打开 {}ms，期间按 {} 处理",
                    n, err.toString(), properties.circuitBreakerOpenMs(),
                    properties.failOpenOnError() ? "放行(fail-open)" : "全挡(fail-closed)");
        } else {
            log.debug("限流计数存储失败（第 {} 次，{}）", n, err.toString());
        }
        return onUnavailable(err.toString(), appNo, canonicalIp);
    }

    /**
     * 存储不可用的统一决策：两层额度同进同退，不做任何本机计数。
     * fail-open=放行（不带计数结论）；fail-closed=挡回，由过滤器回 503。
     */
    private Gate onUnavailable(String reason, String appNo, String canonicalIp) {
        if (properties.failOpenOnError()) {
            log.debug("计数存储不可用（{}），按 fail-open 放行且不补计数 app={} ip={}",
                    reason, appNo, canonicalIp);
            return new Gate(true, true, RateLimitVerdict.pass());
        }
        log.warn("计数存储不可用（{}），按 fail-closed 挡回 app={} ip={}", reason, appNo, canonicalIp);
        return new Gate(false, true, RateLimitVerdict.reject("STORE", 0));
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
