package com.apigw.infrastructure.store;

import com.apigw.common.exception.BizException;
import com.apigw.domain.route.GatewayRoute;
import com.apigw.domain.route.GatewayRule;
import com.apigw.domain.route.RuleTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * 路由配置在 Redis 里的读写封装。
 *
 * 存储结构（一条路由 + 它的全部条件/动作 = Hash 里的一个 field）：
 *   key   = apigw:routes               （Hash）
 *   field = routeNo
 *   value = 该路由及其全部子项的 JSON
 *
 * 这样「整树保存、整树删除」天然就是原子的：
 * - 保存只有一次 HSET/HSETNX，删只有一次 HDEL，Redis 单命令不会插进半截，
 *   不可能出现「主记录进了、子记录没进」的残缺路由，也不需要手工回滚；
 * - 读出来永远是一整份完整配置，没有无主的子记录可留。
 *
 * 编号占用用 HSETNX 原子判定；同一条路由的「读版本→写回」用短租约锁串行化，
 * 再配合 version 乐观锁：后到的旧版本提交会被拒，提示「你这份旧了」。
 *
 * 这里只负责序列化与并发控制，业务规则在 {@link GatewayRoute} 聚合里。
 */
@Component
public class RouteStore {

    public static final String ROUTES_KEY = "apigw:routes";

    private static final Duration LOCK_TTL = Duration.ofSeconds(5);
    private static final int LOCK_RETRY = 50;

    /** 释放锁的 Lua：只有锁的持有者（token 对得上）才能删，避免 TTL 边缘误删别人的锁。 */
    private static final RedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public RouteStore(ReactiveStringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    /** 读全部路由（含子项）。 */
    public Flux<GatewayRoute> findAll() {
        return redis.opsForHash().values(ROUTES_KEY)
                .map(v -> deserialize(v.toString()));
    }

    /** 按编号读一条。 */
    public Mono<GatewayRoute> findByRouteNo(String routeNo) {
        return redis.opsForHash().get(ROUTES_KEY, routeNo)
                .map(v -> deserialize(v.toString()));
    }

    /**
     * 新建：用 HSETNX 原子占位——field 不存在才写入。
     *
     * 「先查有没有、再写」在两步之间有窗口，两个人同时建同一个编号会双双成功；
     * HSETNX 把查重和写入合成 Redis 里的一个原子动作，谁先谁赢，后来者直接收占用错误。
     * 停用的路由也占着编号（field 还在），删除才真正释放编号。
     */
    public Mono<GatewayRoute> create(GatewayRoute route) {
        route.setVersion(0);
        return redis.opsForHash().putIfAbsent(ROUTES_KEY, route.getRouteNo(), serialize(route))
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? Mono.just(route)
                        : Mono.error(new BizException(
                                "路由编号已被占用（停用的路由也占号）：" + route.getRouteNo())));
    }

    /**
     * 修改（整树覆盖）。必须显式带上读取时拿到的 version：
     * - 锁保证同一时刻只有一个人在「读版本→写回」；
     * - version 比对保证旧版本提交进不来，写入后版本 +1。
     * 两个人同时改同一条，后到的拿到的版本已变，会收到明确的「你这份旧了」。
     */
    public Mono<GatewayRoute> update(GatewayRoute route) {
        Integer expectVersion = route.getVersion();
        if (expectVersion == null) {
            // 不允许「不带版本就改」，否则等于把乐观锁绕过去，静默覆盖别人的修改
            return Mono.error(new BizException("修改必须带上读取时拿到的版本号 version（首版也要显式传 0），用于并发冲突检测"));
        }
        return withLock(route.getRouteNo(), () ->
                findByRouteNo(route.getRouteNo())
                        .switchIfEmpty(Mono.error(new BizException(404, "路由不存在：" + route.getRouteNo())))
                        .flatMap(existing -> {
                            if (!expectVersion.equals(existing.getVersion())) {
                                return Mono.error(versionConflict(existing.getVersion(), expectVersion));
                            }
                            // id 沿用旧的，编号建后不可改；整树覆盖子项
                            route.setId(existing.getId());
                            route.setVersion(existing.getVersion() + 1);
                            return write(route).thenReturn(route);
                        }));
    }

    /**
     * 删除：先确认存在（删不存在的不算成功，给明确结果），再 HDEL。
     * 条件与动作跟主记录在同一个 field 里，一次 HDEL 整树清掉，不会留无主子记录。
     * 带上 expectVersion 还能拦住「别人先改了、我手里还是旧版却来删」。
     */
    public Mono<Void> delete(String routeNo, Integer expectVersion) {
        return withLock(routeNo, () ->
                findByRouteNo(routeNo)
                        .switchIfEmpty(Mono.error(new BizException(404,
                                "路由不存在，删除未执行：" + routeNo)))
                        .flatMap(existing -> {
                            if (expectVersion != null && !expectVersion.equals(existing.getVersion())) {
                                return Mono.error(versionConflict(existing.getVersion(), expectVersion));
                            }
                            return redis.opsForHash().remove(ROUTES_KEY, routeNo).then();
                        }));
    }

    private BizException versionConflict(int currentVersion, int expectVersion) {
        return new BizException(409, "你这份配置已经旧了（当前版本 " + currentVersion
                + "，你手上是 " + expectVersion + "），请重新拉取后再提交");
    }

    private Mono<Void> write(GatewayRoute route) {
        return redis.opsForHash()
                .put(ROUTES_KEY, route.getRouteNo(), serialize(route))
                .then();
    }

    /** 用一份完整配置替换整个 Hash —— 给后续「配置整体刷新」留的原子入口。 */
    public Mono<Void> replaceAll(Map<String, GatewayRoute> routes) {
        Map<String, String> raw = new TreeMap<>();
        routes.forEach((k, v) -> raw.put(k, serialize(v)));
        return redis.delete(ROUTES_KEY)
                .then(redis.opsForHash().putAll(ROUTES_KEY, raw))
                .then();
    }

    /**
     * 拿一把基于 Redis 的短租约锁（SET NX + TTL + 唯一 token），保证「读版本 → 写回」是临界区。
     * 拿不到就小睡重试，超时抛业务异常，避免无限自旋；释放时用 Lua 比对 token，只删自己的锁。
     */
    private <T> Mono<T> withLock(String routeNo, Supplier<Mono<T>> action) {
        String lockKey = "apigw:lock:route:" + routeNo;
        String token = UUID.randomUUID().toString();
        return tryLock(lockKey, token, 0)
                .flatMap(acquired -> {
                    if (!acquired) {
                        return Mono.error(new BizException("这条路由正被另一个人修改，请稍后重试"));
                    }
                    return action.get()
                            .doFinally(sig -> redis.execute(UNLOCK_SCRIPT, List.of(lockKey), List.of(token))
                                    .subscribe());
                });
    }

    private Mono<Boolean> tryLock(String lockKey, String token, int attempt) {
        return redis.opsForValue()
                .setIfAbsent(lockKey, token, LOCK_TTL)
                .flatMap(ok -> {
                    if (ok || attempt >= LOCK_RETRY) {
                        return Mono.just(ok);
                    }
                    return Mono.delay(Duration.ofMillis(20))
                            .then(tryLock(lockKey, token, attempt + 1));
                });
    }

    // ---- 序列化：不用 Java 原生序列化，存 JSON，便于人工排查与后续版本迁移 ----

    public String serialize(GatewayRoute route) {
        try {
            return objectMapper.writeValueAsString(Dto.from(route));
        } catch (Exception e) {
            throw new BizException("路由序列化失败：" + e.getMessage());
        }
    }

    public GatewayRoute deserialize(String json) {
        try {
            Dto dto = objectMapper.readValue(json, new TypeReference<Dto>() {
            });
            return dto.toDomain();
        } catch (Exception e) {
            throw new BizException("路由反序列化失败：" + e.getMessage());
        }
    }

    /** 落 Redis 的形状：字段与领域对象一致，直接复用领域模型的公开 getter/setter。 */
    public static class Dto {
        public String id;
        public String routeNo;
        public String name;
        public String upstream;
        public Integer enabled;
        public Integer authRequired;
        public String remark;
        public Integer version;
        public List<RuleDto> conditions = new ArrayList<>();
        public List<RuleDto> actions = new ArrayList<>();

        static Dto from(GatewayRoute r) {
            Dto d = new Dto();
            d.id = r.getId();
            d.routeNo = r.getRouteNo();
            d.name = r.getName();
            d.upstream = r.getUpstream();
            d.enabled = r.getEnabled();
            d.authRequired = r.getAuthRequired();
            d.remark = r.getRemark();
            d.version = r.getVersion();
            d.conditions = r.getConditions().stream().map(RuleDto::from).toList();
            d.actions = r.getActions().stream().map(RuleDto::from).toList();
            return d;
        }

        GatewayRoute toDomain() {
            GatewayRoute r = GatewayRoute.create(routeNo, name, upstream, enabled, remark);
            // 旧配置里没有这个字段：null 进 changeAuthRequired 按 0（开放）落，向后兼容
            r.changeAuthRequired(authRequired);
            r.setId(id);
            r.setVersion(version == null ? 0 : version);
            r.replaceRules(
                    conditions == null ? List.of() : conditions.stream().map(RuleDto::toDomain).toList(),
                    actions == null ? List.of() : actions.stream().map(RuleDto::toDomain).toList());
            r.getConditions().forEach(x -> x.setRuleKind(RuleTypes.KIND_CONDITION));
            r.getActions().forEach(x -> x.setRuleKind(RuleTypes.KIND_ACTION));
            return r;
        }
    }

    /** 子项在 Redis 里的形状。 */
    public static class RuleDto {
        public String id;
        public String stage;
        public String type;
        public String name;
        public String value;
        public Integer sortNo;

        static RuleDto from(GatewayRule g) {
            RuleDto d = new RuleDto();
            d.id = g.getId();
            d.stage = g.getStage();
            d.type = g.getType();
            d.name = g.getName();
            d.value = g.getValue();
            d.sortNo = g.getSortNo();
            return d;
        }

        GatewayRule toDomain() {
            return GatewayRule.create(stage, type, name, value, sortNo);
        }
    }

    /** 按顺序号排好的子项（对外返回时统一排序）。 */
    public static List<GatewayRule> sorted(List<GatewayRule> rules) {
        List<GatewayRule> copy = new ArrayList<>(rules);
        copy.sort(Comparator.comparing(GatewayRule::getSortNo, Comparator.nullsLast(Integer::compareTo)));
        return copy;
    }

    /** 供管理接口做「按编号模糊找」用：把 keyword 归一化，空串视为不过滤。 */
    public static String normalizeKeyword(String keyword) {
        if (keyword == null) {
            return null;
        }
        String k = keyword.trim();
        return k.isEmpty() ? null : k;
    }

    /** 编号集合，供统计用。 */
    public Mono<Set<String>> allRouteNos() {
        return redis.opsForHash().keys(ROUTES_KEY).collectList()
                .map(list -> new java.util.HashSet<>(list.stream().map(Object::toString).toList()));
    }
}
