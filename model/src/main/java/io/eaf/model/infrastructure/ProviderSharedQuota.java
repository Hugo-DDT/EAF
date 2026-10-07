package io.eaf.model.infrastructure;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** PostgreSQL 共享已登记 Provider 的在途 HTTP 槽；过期租约只隔离，不按 TTL 回收，以免远端请求仍在执行时重复分配。 */
@Component
public final class ProviderSharedQuota implements MeterBinder {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final String mode;
    private final int maxConcurrent;
    private final Duration leaseDuration;
    private final Duration heartbeatInterval;
    private final boolean maintenanceResize;
    private final UUID ownerIncarnation = UUID.randomUUID();
    private final ScheduledExecutorService maintenance = Executors.newScheduledThreadPool(2, task -> {
        var thread = new Thread(task, "eaf-provider-quota-maintenance");
        thread.setDaemon(true);
        return thread;
    });
    private final ConcurrentHashMap<UUID, ScheduledFuture<?>> releaseRetries = new ConcurrentHashMap<>();
    private final AtomicInteger free = new AtomicInteger();
    private final AtomicInteger held = new AtomicInteger();
    private final AtomicInteger quarantined = new AtomicInteger();
    private volatile Counter coordinationFailures;

    public ProviderSharedQuota(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
                               @Value("${eaf.model.quota-mode:local}") String mode,
                               @Value("${eaf.model.shared-max-concurrent:16}") int maxConcurrent,
                               @Value("${eaf.model.shared-lease-duration:PT90S}") Duration leaseDuration,
                               @Value("${eaf.model.shared-heartbeat-interval:PT10S}") Duration heartbeatInterval,
                               @Value("${eaf.p20.maintenance-command:}") String maintenanceCommand) {
        this.jdbc = jdbc;
        this.transactions = new TransactionTemplate(transactionManager);
        this.mode = mode == null ? "" : mode.trim().toLowerCase(java.util.Locale.ROOT);
        this.maxConcurrent = maxConcurrent;
        this.leaseDuration = leaseDuration;
        this.heartbeatInterval = heartbeatInterval;
        this.maintenanceResize = "model-pool-set".equals(maintenanceCommand);
        if (!List.of("local", "postgres").contains(this.mode) || maxConcurrent <= 0
                || !positive(leaseDuration) || !positive(heartbeatInterval)
                || heartbeatInterval.multipliedBy(3).compareTo(leaseDuration) > 0)
            throw new IllegalArgumentException("EAF Model quota mode and lease settings are invalid.");
    }

    @PostConstruct
    void initialize() {
        if (!enabled()) return;
        transactions.executeWithoutResult(status -> {
            jdbc.query("select pg_advisory_xact_lock(hashtext('eaf.model.provider.shared.slots.v1'), 0)",
                    rs -> { if (rs.next()) rs.getObject(1); return null; });
            var count = jdbc.queryForObject("select count(*) from model.outbound_slot", Integer.class);
            if (count == 0 && !maintenanceResize) jdbc.update("insert into model.outbound_slot(slot_no, state) "
                    + "select n, 'FREE' from generate_series(1, ?) n", maxConcurrent);
            else if (!maintenanceResize && count != maxConcurrent)
                throw new IllegalStateException("EAF Model 共享 Provider 槽数与数据库不一致；停止所有节点后维护。");
        });
        quarantineExpired();
        maintenance.scheduleWithFixedDelay(this::quarantineExpired, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        registry.gauge("eaf.model.shared.outbound", List.of(io.micrometer.core.instrument.Tag.of("state", "free")), free);
        registry.gauge("eaf.model.shared.outbound", List.of(io.micrometer.core.instrument.Tag.of("state", "held")), held);
        registry.gauge("eaf.model.shared.outbound", List.of(io.micrometer.core.instrument.Tag.of("state", "quarantined")), quarantined);
        coordinationFailures = Counter.builder("eaf.model.shared.outbound.coordination.failures").register(registry);
    }

    boolean enabled() { return "postgres".equals(mode); }

    SharedLease tryAcquire() throws ProviderCapacityExceededException, ProviderQuotaUnavailableException {
        if (!enabled()) return null;
        try {
            var lease = transactions.execute(status -> {
                var slot = jdbc.query("select slot_no from model.outbound_slot where state = 'FREE' "
                                + "order by slot_no limit 1 for update skip locked",
                        rs -> rs.next() ? rs.getInt(1) : null);
                if (slot == null) return null;
                var leaseId = UUID.randomUUID();
                var updated = jdbc.update("update model.outbound_slot set state = 'HELD', lease_id = ?, owner_incarnation = ?, "
                                + "lease_until = clock_timestamp() + (? * interval '1 millisecond'), updated_at = clock_timestamp() "
                                + "where slot_no = ? and state = 'FREE'",
                        leaseId, ownerIncarnation, leaseDuration.toMillis(), slot);
                return updated == 1 ? new SharedLease(slot, leaseId, ownerIncarnation) : null;
            });
            refreshCounts();
            if (lease == null) throw new ProviderCapacityExceededException();
            return lease;
        } catch (ProviderCapacityExceededException failure) {
            throw failure;
        } catch (RuntimeException unavailable) {
            countCoordinationFailure();
            throw new ProviderQuotaUnavailableException();
        }
    }

    ScheduledFuture<?> heartbeat(SharedLease lease, Runnable lost) {
        if (lease == null) return null;
        return maintenance.scheduleWithFixedDelay(() -> {
            try {
                if (!renew(lease)) {
                    quarantine(lease);
                    lost.run();
                }
            } catch (RuntimeException unavailable) {
                countCoordinationFailure();
                quarantine(lease);
                lost.run();
            }
        }, heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean renew(SharedLease lease) {
        var changed = jdbc.update("update model.outbound_slot set lease_until = clock_timestamp() + (? * interval '1 millisecond'), "
                        + "updated_at = clock_timestamp() where slot_no = ? and state = 'HELD' and lease_id = ? "
                        + "and owner_incarnation = ? and lease_until > clock_timestamp()",
                leaseDuration.toMillis(), lease.slotNo(), lease.leaseId(), lease.ownerIncarnation());
        return changed == 1;
    }

    void quarantine(SharedLease lease) {
        if (lease == null) return;
        try {
            jdbc.update("update model.outbound_slot set state = 'QUARANTINED', updated_at = clock_timestamp() "
                            + "where slot_no = ? and state = 'HELD' and lease_id = ? and owner_incarnation = ?",
                    lease.slotNo(), lease.leaseId(), lease.ownerIncarnation());
            refreshCounts();
        } catch (RuntimeException unavailable) {
            countCoordinationFailure();
        }
    }

    void releaseAfterTransportClosed(SharedLease lease) {
        if (lease == null) return;
        try {
            release(lease);
        } catch (RuntimeException unavailable) {
            countCoordinationFailure();
            releaseRetries.computeIfAbsent(lease.leaseId(), id -> maintenance.scheduleWithFixedDelay(() -> {
                try {
                    release(lease);
                    var retry = releaseRetries.remove(id);
                    if (retry != null) retry.cancel(false);
                } catch (RuntimeException stillUnavailable) {
                    countCoordinationFailure();
                }
            }, 2, 2, TimeUnit.SECONDS));
        }
    }

    private void release(SharedLease lease) {
        jdbc.update("update model.outbound_slot set state = 'FREE', lease_id = null, owner_incarnation = null, "
                        + "lease_until = null, updated_at = clock_timestamp() where slot_no = ? "
                        + "and state in ('HELD','QUARANTINED') and lease_id = ? and owner_incarnation = ?",
                lease.slotNo(), lease.leaseId(), lease.ownerIncarnation());
        refreshCounts();
    }

    private void quarantineExpired() {
        if (!enabled()) return;
        try {
            // TTL 只表明本地续约失联；没有 HTTP 已结束的证据时继续占用集群容量。
            jdbc.update("update model.outbound_slot set state = 'QUARANTINED', updated_at = clock_timestamp() "
                    + "where state = 'HELD' and lease_until <= clock_timestamp()");
            refreshCounts();
        } catch (RuntimeException unavailable) {
            countCoordinationFailure();
        }
    }

    private void refreshCounts() {
        var counts = jdbc.query("select state, count(*) as count from model.outbound_slot group by state",
                rs -> {
                    var result = new int[3];
                    while (rs.next()) result[switch (rs.getString("state")) {
                        case "FREE" -> 0;
                        case "HELD" -> 1;
                        default -> 2;
                    }] = rs.getInt("count");
                    return result;
                });
        free.set(counts[0]); held.set(counts[1]); quarantined.set(counts[2]);
    }

    public List<QuarantinedSlot> inspectQuarantined() {
        if (!enabled()) throw new IllegalStateException("仅 postgres quota mode 有共享槽。");
        return jdbc.query("select slot_no, lease_id, owner_incarnation, lease_until from model.outbound_slot "
                        + "where state = 'QUARANTINED' order by slot_no",
                (rs, row) -> new QuarantinedSlot(rs.getInt("slot_no"), rs.getObject("lease_id", UUID.class),
                        rs.getObject("owner_incarnation", UUID.class), rs.getTimestamp("lease_until").toInstant()));
    }

    public boolean clearQuarantined(int slotNo, UUID expectedLeaseId, String operator, String reason) {
        if (!enabled() || slotNo <= 0 || expectedLeaseId == null || operator == null || operator.isBlank()
                || operator.length() > 120 || reason == null || reason.isBlank() || reason.length() > 500)
            throw new IllegalArgumentException("离线清理必须指定槽、原 leaseId、操作者和理由。");
        var changed = transactions.execute(status -> jdbc.update("update model.outbound_slot set state = 'FREE', lease_id = null, "
                        + "owner_incarnation = null, lease_until = null, updated_at = clock_timestamp(), cleared_by = ?, "
                        + "cleared_reason = ?, cleared_at = clock_timestamp() where slot_no = ? and state = 'QUARANTINED' and lease_id = ?",
                operator.trim(), reason.trim(), slotNo, expectedLeaseId));
        refreshCounts();
        return changed != null && changed == 1;
    }

    /** 仅由停机维护命令调用；要求期望容量匹配且所有旧槽都为空闲，避免运行中改变公共并发上限。 */
    public int resizePool(int expectedCurrentSlots, int desiredSlots) {
        if (!enabled() || !maintenanceResize || expectedCurrentSlots <= 0 || desiredSlots <= 0)
            throw new IllegalStateException("共享 Model 槽容量只能由显式停机维护命令调整。");
        var changed = transactions.execute(status -> {
            jdbc.query("select pg_advisory_xact_lock(hashtext('eaf.model.provider.shared.slots.v1'), 0)",
                    rs -> { if (rs.next()) rs.getObject(1); return null; });
            var current = jdbc.queryForObject("select count(*) from model.outbound_slot", Integer.class);
            if (current == null || current != expectedCurrentSlots)
                throw new IllegalStateException("共享 Model 槽数量已变化；重新查询后再操作。");
            var occupied = jdbc.queryForObject("select count(*) from model.outbound_slot where state <> 'FREE'", Integer.class);
            if (occupied == null || occupied != 0)
                throw new IllegalStateException("共享 Model 槽仍有 HELD/QUARANTINED 占用，不能调整容量。");
            jdbc.update("delete from model.outbound_slot where slot_no > ? and state = 'FREE'", desiredSlots);
            jdbc.update("insert into model.outbound_slot(slot_no, state) "
                            + "select n, 'FREE' from generate_series(1, ?) n "
                            + "where not exists (select 1 from model.outbound_slot s where s.slot_no = n)",
                    desiredSlots);
            return jdbc.queryForObject("select count(*) from model.outbound_slot", Integer.class);
        });
        refreshCounts();
        return changed == null ? 0 : changed;
    }

    private void countCoordinationFailure() {
        var counter = coordinationFailures;
        if (counter != null) counter.increment();
    }

    private static boolean positive(Duration value) { return value != null && !value.isNegative() && !value.isZero(); }

    @PreDestroy
    void shutdown() { maintenance.shutdownNow(); }

    record SharedLease(int slotNo, UUID leaseId, UUID ownerIncarnation) { }
    public record QuarantinedSlot(int slotNo, UUID leaseId, UUID ownerIncarnation, java.time.Instant leaseUntil) { }
}
