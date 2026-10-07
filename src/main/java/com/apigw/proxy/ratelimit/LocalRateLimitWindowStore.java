package com.apigw.proxy.ratelimit;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内固定窗口计数：计数存储抖动期间的兜底，先把额度挡在本机，别让流量一股脑放到上游。
 *
 * <p>与 Redis 实现同一套判定口径：两层（应用总量 / 应用下来源地址）都判，任一层超了立即拒，
 * 被拒请求不占名额，窗口一到换一批计数（旧窗口由 {@code compute} 覆盖回收）。
 * 它只回答「本进程这一份额度够不够」，不承担全局口径——那仍由共享存储负责。
 */
public class LocalRateLimitWindowStore implements RateLimitWindowStore {

    private static final class Window {
        final long startMs;
        int appCount;
        final Map<String, Integer> ipCounts = new ConcurrentHashMap<>();

        Window(long startMs) {
            this.startMs = startMs;
        }
    }

    private final long windowMs;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public LocalRateLimitWindowStore(long windowSeconds) {
        this.windowMs = Math.max(1L, windowSeconds) * 1000L;
    }

    @Override
    public Mono<RateLimitVerdict> checkAndConsume(String appNo, String canonicalIp,
                                                  Integer appLimit, Integer ipLimit) {
        return Mono.just(consume(appNo, canonicalIp, appLimit, ipLimit));
    }

    /** 同步版：给不回到响应式链路的调用方（纯内存计算，不阻塞）。 */
    public RateLimitVerdict consume(String appNo, String canonicalIp,
                                    Integer appLimit, Integer ipLimit) {
        long now = System.currentTimeMillis();
        long windowStart = now - (now % windowMs);
        Window window = windows.compute(appNo,
                (k, cur) -> cur == null || cur.startMs != windowStart ? new Window(windowStart) : cur);
        synchronized (window) {
            if (appLimit != null && window.appCount >= appLimit) {
                return RateLimitVerdict.reject("APP", secondsLeft(windowStart, now));
            }
            String ipKey = canonicalIp == null ? "unknown" : canonicalIp;
            int ipCount = window.ipCounts.getOrDefault(ipKey, 0);
            if (ipLimit != null && ipCount >= ipLimit) {
                return RateLimitVerdict.reject("IP", secondsLeft(windowStart, now));
            }
            window.appCount++;
            window.ipCounts.merge(ipKey, 1, Integer::sum);
            return RateLimitVerdict.pass();
        }
    }

    /** 距窗口结束还要几秒（至少 1s，与 Redis 实现的 Retry-After 口径一致）。 */
    private long secondsLeft(long windowStart, long now) {
        long leftMs = windowStart + windowMs - now;
        return Math.max(1L, (leftMs + 999L) / 1000L);
    }
}
