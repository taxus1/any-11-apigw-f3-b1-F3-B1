package com.apigw.proxy.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 限流可调参数（前缀 {@code apigw.rate-limit}）。
 *
 * @param enabled              是否开启限流。与 app-auth 独立：开限流只装限流这一套；
 *                             计数存储用 Redis（路由配置已在用），额度配置用共享数据源，
 *                             所以开限流会同时需要 Redis 与库。
 * @param refreshInterval      额度快照兜底轮询周期：多实例下别的实例改了额度靠它收敛，
 *                             本实例改动主要靠事件即时生效。
 * @param redisTimeout         单次计数读写（整段判定 Lua）的等待上限。超时即视为计数不可用，
 *                             <b>绝不死等</b>：到点立刻按 {@link #failOpenOnError} 决策，
 *                             避免 Redis 抖动把每个请求都拖在网关上。
 * @param failOpenOnError      计数存储暂时不可用（超时/连不上/执行出错/熔断打开/半开非探活）时的总策略：
 *                             true（默认）= 这一小会儿放行（fail-open）且<b>不补计数</b>——存储不可用
 *                             无处可计，也刻意不做本机兜底（本机各算一份 = 额度按实例数放大）；
 *                             优先保网关不被拖垮、上游可能短暂承压但有熔断快速摘流；
 *                             false = fail-closed 全挡回 503。
 * @param circuitBreakerThreshold 连续多少次计数失败后熔断打开：熔断期间根本不再发 Redis 请求，
 *                             直接按 {@link #failOpenOnError} 决策（连那几十毫秒都不等），
 *                             把依赖故障与转发线程彻底隔离。
 * @param circuitBreakerOpenMs 熔断打开多久后进入半开：放一笔真实请求探活，成功则闭合。
 * @param windowSeconds        窗口长度（秒）。需求口径固定为一分钟，默认 60；做成参数仅供测试
 *                             加速窗口轮转，生产不应改。
 */
@ConfigurationProperties(prefix = "apigw.rate-limit")
public record RateLimitProperties(boolean enabled,
                                  Duration refreshInterval,
                                  long refreshIntervalMs,
                                  Duration redisTimeout,
                                  boolean failOpenOnError,
                                  int circuitBreakerThreshold,
                                  long circuitBreakerOpenMs,
                                  long windowSeconds) {

    public RateLimitProperties {
        if (refreshInterval == null || refreshInterval.isZero() || refreshInterval.isNegative()) {
            refreshInterval = Duration.ofSeconds(10);
        }
        if (refreshIntervalMs <= 0) {
            refreshIntervalMs = refreshInterval.toMillis();
        }
        if (redisTimeout == null || redisTimeout.isZero() || redisTimeout.isNegative()) {
            redisTimeout = Duration.ofMillis(100);
        }
        if (circuitBreakerThreshold <= 0) {
            circuitBreakerThreshold = 5;
        }
        if (circuitBreakerOpenMs <= 0) {
            circuitBreakerOpenMs = 5_000;
        }
        if (windowSeconds <= 0) {
            windowSeconds = 60;
        }
    }

    public static RateLimitProperties defaults() {
        return new RateLimitProperties(false, Duration.ofSeconds(10), 10_000,
                Duration.ofMillis(100), true, 5, 5_000, 60);
    }
}
