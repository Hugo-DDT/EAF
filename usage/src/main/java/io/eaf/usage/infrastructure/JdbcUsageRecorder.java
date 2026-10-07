package io.eaf.usage.infrastructure;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.usage.api.UsageOperationsCursor;
import io.eaf.usage.api.UsageOperationsItem;
import io.eaf.usage.api.UsageOperationsPage;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.SpendReservation;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcUsageRecorder implements UsageRecorder {
    private static final Set<String> COST_STATUSES = Set.of("KNOWN", "ESTIMATED", "BILLED", "UNKNOWN_USAGE", "UNKNOWN_PRICE");
    private static final String COLUMNS = "tenant_id, workspace_id, task_id, run_id, source, provider, model, input_tokens, output_tokens, "
            + "usage_status, estimated_cost, cost_currency, cost_status, cost_source, reserved_tokens, status, error_code, started_at, ended_at, call_no, "
            + "call_key, call_type, scope_type, scope_id, price_version, price_source_version, price_effective_at, billing_unit, "
            + "input_price_per_million, output_price_per_million, request_price, actual_cost, actual_cost_currency, billing_source";
    private static final RowMapper<UsageRecord> ROW_MAPPER = JdbcUsageRecorder::map;
    private static final RowMapper<Price> PRICE_MAPPER = (rs, row) -> new Price(rs.getString("price_version"),
            rs.getString("source"), rs.getString("source_version"), rs.getString("currency"), rs.getString("billing_unit"),
            rs.getBigDecimal("input_price_per_million"), rs.getBigDecimal("output_price_per_million"),
            rs.getBigDecimal("request_price"), rs.getTimestamp("effective_at").toInstant());
    private static final BigDecimal MILLION = new BigDecimal("1000000");
    private static final String SELECT_COLUMNS = "select " + COLUMNS + " from usage.model_usage ";
    private final JdbcTemplate jdbc;
    private final MeterRegistry metrics;
    private final WorkspaceAuthorization workspaces;

    public JdbcUsageRecorder(JdbcTemplate jdbc, MeterRegistry metrics, WorkspaceAuthorization workspaces) {
        this.jdbc = jdbc;
        this.metrics = metrics;
        this.workspaces = workspaces;
    }

    // 首次写入固定精确价格快照；重放只接受同一观测，绝不按后来价格重算历史调用。
    @Override
    @Transactional
    public void record(UsageRecord record) {
        validateRecord(record);
        var price = reservedPrice(record.callKey());
        if (price == null) price = activePrice(record);
        var estimate = estimate(record, price);
        var billed = record.actualCost() != null;
        var actualCost = billed ? money(record.actualCost()) : null;
        var actualCurrency = billed ? record.actualCostCurrency() : null;
        var billingSource = billed ? record.billingSource() : null;
        if (billed) validateBilling(actualCost, actualCurrency, billingSource);
        var costStatus = billed ? "BILLED" : estimate.status();
        var estimatedCost = "ESTIMATED".equals(estimate.status()) ? estimate.amount() : null;
        try {
            var inserted = jdbc.update("insert into usage.model_usage(id, tenant_id, workspace_id, task_id, run_id, call_no, call_key, call_type, scope_type, scope_id, source, provider, model, input_tokens, output_tokens, usage_status, estimated_cost, cost_currency, cost_status, cost_source, price_version, price_source_version, price_effective_at, billing_unit, input_price_per_million, output_price_per_million, request_price, actual_cost, actual_cost_currency, billing_source, reserved_tokens, status, error_code, started_at, ended_at) "
                            + "values (gen_random_uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict (call_key) do nothing",
                    record.tenantId(), record.workspaceId(), record.taskId(), record.runId(), record.callNo(), record.callKey(),
                    record.callType(), record.scopeType(), record.scopeId(), record.source(), record.provider(), record.model(),
                    record.inputTokens(), record.outputTokens(), record.usageStatus(), estimatedCost,
                    price == null ? null : price.currency(), costStatus,
                    price == null ? null : price.source(), price == null ? null : price.version(),
                    price == null ? null : price.sourceVersion(), price == null ? null : Timestamp.from(price.effectiveAt()),
                    price == null ? null : price.billingUnit(), price == null ? null : price.inputRate(),
                    price == null ? null : price.outputRate(), price == null ? null : price.requestPrice(), actualCost,
                    actualCurrency, billingSource, record.reservedTokens(), record.status(), record.errorCode(),
                    Timestamp.from(record.startedAt()), record.endedAt() == null ? null : Timestamp.from(record.endedAt()));
            if (inserted == 0) verifyReplay(record);
            settleReservedCall(record, estimate);
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("USAGE_CALL_CONFLICT", "Usage 调用键或 Task 调用序号已绑定到其他记录。");
        }
    }

    @Override
    public void markFailed(UUID taskId, UUID runId, int callNo, String errorCode) {
        jdbc.update("update usage.model_usage set status = 'FAILED', error_code = ? where task_id = ? and run_id = ? and call_no = ?",
                errorCode, taskId, runId, callNo);
    }

    @Override
    @Transactional
    public void recordBilledCost(String callKey, BigDecimal amount, String currency, String billingSource) {
        validateBilling(amount, currency, billingSource);
        var rows = jdbc.query(SELECT_COLUMNS + "where call_key = ? for update", ROW_MAPPER, callKey);
        if (rows.isEmpty()) throw EafException.notFound();
        var current = rows.getFirst();
        if (current.actualCost() != null) {
            if (current.actualCost().compareTo(money(amount)) == 0
                    && Objects.equals(current.actualCostCurrency(), currency)
                    && Objects.equals(current.billingSource(), billingSource)) return;
            throw EafException.conflict("BILLING_FACT_CONFLICT", "同一 Usage 调用已绑定不同账单事实。");
        }
        jdbc.update("update usage.model_usage set actual_cost = ?, actual_cost_currency = ?, billing_source = ?, cost_status = 'BILLED' where call_key = ?",
                money(amount), currency, billingSource, callKey);
        reconcileBilledSpend(callKey, money(amount), currency);
    }

    @Override
    @Transactional
    public SpendReservation reserveSpend(ReserveSpendCommand command) {
        validateSpendCommand(command);
        var scope = lockSpendScope(command, command.limitCurrency());
        if (scope == null) return denied("SPEND_SCOPE_INVALID", null, command.limitCurrency());
        if (scope.limitAmount().compareTo(money(command.limitAmount())) != 0
                || !Objects.equals(scope.currency(), command.limitCurrency())) {
            stopScope(command, "SPEND_SCOPE_CONFIG_CHANGED");
            return denied("SPEND_SCOPE_CONFIG_CHANGED", null, scope.currency());
        }
        if (!"ACTIVE".equals(scope.status()))
            return denied(scope.stopReason(), null, scope.currency());

        var price = activePrice(command.provider(), command.model(), command.callType(), Instant.now());
        if (price == null) {
            stopScope(command, "UNKNOWN_PRICE");
            return denied("UNKNOWN_PRICE", null, scope.currency());
        }
        if (!Objects.equals(price.currency(), scope.currency())) {
            stopScope(command, "SPEND_CURRENCY_MISMATCH");
            return denied("SPEND_CURRENCY_MISMATCH", null, scope.currency());
        }
        var amount = reserveUpperBound(price, command.maxTokens());
        if (amount == null) {
            stopScope(command, "UNKNOWN_COST_BOUND");
            return denied("UNKNOWN_COST_BOUND", null, scope.currency());
        }
        var existing = jdbc.queryForObject("select count(*) from usage.spend_reservation where call_key = ?", Integer.class, command.callKey());
        if (existing != null && existing > 0) {
            stopScope(command, "SPEND_CALL_REPLAY");
            return denied("SPEND_CALL_REPLAY", amount, scope.currency());
        }
        if (scope.spentAmount().add(scope.reservedAmount()).add(amount).compareTo(scope.limitAmount()) > 0) {
            stopScope(command, "COST_CAP_REACHED");
            return denied("COST_CAP_REACHED", amount, scope.currency());
        }
        var inserted = jdbc.update("insert into usage.spend_reservation(call_key, tenant_id, workspace_id, scope_type, scope_id, provider, model, call_type, max_tokens, price_version, price_source, price_source_version, price_effective_at, billing_unit, input_price_per_million, output_price_per_million, request_price, currency, reserved_amount, state) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RESERVED') on conflict (call_key) do nothing",
                command.callKey(), command.tenantId(), command.workspaceId(), command.scopeType(), command.scopeId(),
                command.provider(), command.model(), command.callType(), command.maxTokens(), price.version(), price.source(),
                price.sourceVersion(), Timestamp.from(price.effectiveAt()), price.billingUnit(), price.inputRate(),
                price.outputRate(), price.requestPrice(), price.currency(), amount);
        if (inserted == 0) {
            stopScope(command, "SPEND_CALL_REPLAY");
            return denied("SPEND_CALL_REPLAY", amount, scope.currency());
        }
        jdbc.update("update usage.spend_scope set reserved_amount = reserved_amount + ?, updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ?",
                amount, command.tenantId(), command.workspaceId(), command.scopeType(), command.scopeId());
        return new SpendReservation(true, null, amount, scope.currency());
    }

    @Override
    @Transactional
    public void releaseSpend(String callKey) {
        var reference = spendReference(callKey);
        if (reference == null) return;
        var scope = lockSpendScope(reference);
        var reservation = lockSpendReservation(callKey);
        if (reservation == null || !"RESERVED".equals(reservation.state())) return;
        jdbc.update("update usage.spend_scope set reserved_amount = greatest(0, reserved_amount - ?), updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ?",
                reservation.reservedAmount(), reference.tenantId(), reference.workspaceId(), reference.scopeType(), reference.scopeId());
        jdbc.update("update usage.spend_reservation set state = 'RELEASED', settled_at = now() where call_key = ? and state = 'RESERVED'", callKey);
    }

    @Override
    @Transactional
    public void stopSpendScope(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId, String reason) {
        if (tenantId == null || workspaceId == null || scopeType == null || scopeId == null
                || reason == null || !reason.matches("[A-Z_]{1,60}"))
            throw EafException.invalid("金额预算停止原因无效。");
        jdbc.update("update usage.spend_scope set status = 'STOPPED', stop_reason = ?, updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ? and status = 'ACTIVE'",
                reason, tenantId, workspaceId, scopeType, scopeId);
    }

    @Override
    public List<UsageRecord> findForTask(UUID tenantId, UUID workspaceId, UUID taskId) {
        return jdbc.query(SELECT_COLUMNS + "where tenant_id = ? and workspace_id = ? and task_id = ? order by started_at, call_no, call_key",
                ROW_MAPPER, tenantId, workspaceId, taskId);
    }

    @Override
    public List<UsageRecord> findForScope(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId) {
        return jdbc.query(SELECT_COLUMNS + "where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ? order by started_at, call_key",
                ROW_MAPPER, tenantId, workspaceId, scopeType, scopeId);
    }

    @Override
    public UsageOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                              Set<String> costStatuses, Instant startedAfter,
                                              UsageOperationsCursor cursor, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "usage:read");
        if (pageSize < 1 || pageSize > 100 || (cursor != null && (cursor.startedAt() == null || cursor.usageId() == null))
                || statuses != null && statuses.stream().anyMatch(value -> value == null || !value.matches("[A-Z_]{1,40}"))
                || costStatuses != null && !COST_STATUSES.containsAll(costStatuses))
            throw EafException.invalid("Usage 运维列表分页或状态过滤无效。");
        var where = new StringBuilder(" where tenant_id = ? and workspace_id = ?");
        var filters = new java.util.ArrayList<Object>();
        filters.add(access.tenantId());
        filters.add(workspaceId);
        appendFilter(where, filters, "status", statuses);
        appendFilter(where, filters, "cost_status", costStatuses);
        if (startedAfter != null) {
            where.append(" and started_at >= ?");
            filters.add(Timestamp.from(startedAfter));
        }
        var totalSize = jdbc.queryForObject("select count(*) from usage.model_usage" + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new java.util.ArrayList<>(filters);
        if (cursor != null) {
            pageWhere.append(" and (started_at, id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursor.startedAt()));
            pageArgs.add(cursor.usageId());
        }
        pageArgs.add(pageSize + 1);
        // 查询列排除调用键、价格构成和正文，只保留核对用量与费用状态所需字段。
        var selected = jdbc.query("select id, task_id, run_id, source, provider, model, call_type, status, usage_status, cost_status, input_tokens, output_tokens, estimated_cost, actual_cost, coalesce(actual_cost_currency, cost_currency) currency, error_code, started_at, ended_at "
                        + "from usage.model_usage" + pageWhere + " order by started_at desc, id desc limit ?",
                (rs, row) -> new UsageOperationsItem(rs.getObject("id", UUID.class), rs.getObject("task_id", UUID.class),
                        rs.getObject("run_id", UUID.class), rs.getString("source"), rs.getString("provider"),
                        rs.getString("model"), rs.getString("call_type"), rs.getString("status"),
                        rs.getString("usage_status"), rs.getString("cost_status"),
                        (Integer) rs.getObject("input_tokens"), (Integer) rs.getObject("output_tokens"),
                        rs.getBigDecimal("estimated_cost"), rs.getBigDecimal("actual_cost"), rs.getString("currency"),
                        rs.getString("error_code"), rs.getTimestamp("started_at").toInstant(),
                        rs.getTimestamp("ended_at") == null ? null : rs.getTimestamp("ended_at").toInstant()),
                pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        var next = last == null ? null : new UsageOperationsCursor(last.startedAt(), last.usageId());
        return new UsageOperationsPage(items, totalSize == null ? 0 : totalSize, next);
    }

    private void appendFilter(StringBuilder where, List<Object> filters, String column, Set<String> values) {
        if (values == null) return;
        if (values.isEmpty()) {
            where.append(" and 1 = 0");
            return;
        }
        where.append(" and ").append(column).append(" in (")
                .append(String.join(",", java.util.Collections.nCopies(values.size(), "?"))).append(')');
        values.forEach(filters::add);
    }

    private void verifyReplay(UsageRecord request) {
        var rows = jdbc.query(SELECT_COLUMNS + "where call_key = ?", ROW_MAPPER, request.callKey());
        if (rows.isEmpty() || !sameObservation(rows.getFirst(), request))
            throw EafException.conflict("USAGE_CALL_CONFLICT", "相同稳定调用键对应不同的 Usage 事实。");
    }

    private Price activePrice(UsageRecord record) {
        if (record.provider() == null || record.model() == null || record.callType() == null || record.startedAt() == null)
            return null;
        return activePrice(record.provider(), record.model(), record.callType(), record.startedAt());
    }

    private Price activePrice(String provider, String model, String callType, Instant atTime) {
        var at = Timestamp.from(atTime);
        var rows = jdbc.query("select price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, request_price, effective_at "
                        + "from usage.price_schedule where provider = ? and model = ? and call_type = ? and effective_at <= ? "
                        + "and (expires_at is null or expires_at > ?) order by effective_at desc limit 1",
                PRICE_MAPPER, provider, model, callType, at, at);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private Price reservedPrice(String callKey) {
        var rows = jdbc.query("select price_version, price_source as source, price_source_version as source_version, currency, billing_unit, input_price_per_million, output_price_per_million, request_price, price_effective_at as effective_at from usage.spend_reservation where call_key = ?",
                PRICE_MAPPER, callKey);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private void validateSpendCommand(ReserveSpendCommand c) {
        if (c == null || c.tenantId() == null || c.workspaceId() == null || c.scopeType() == null
                || !List.of("TASK", "WORKFLOW", "EVALUATION", "JOB").contains(c.scopeType())
                || c.scopeId() == null || c.callKey() == null || c.callKey().isBlank() || c.callKey().length() > 240
                || c.provider() == null || c.provider().isBlank() || c.model() == null || c.model().isBlank()
                || c.callType() == null || c.maxTokens() < 0 || c.limitAmount() == null || c.limitAmount().signum() <= 0
                || c.limitCurrency() == null || !c.limitCurrency().matches("[A-Z]{3}"))
            throw EafException.invalid("金额预留必须绑定服务器预算、计价身份、稳定调用键和币种。");
    }

    private SpendScope lockSpendScope(ReserveSpendCommand c, String currency) {
        jdbc.update("insert into usage.spend_scope(tenant_id, workspace_id, scope_type, scope_id, currency, limit_amount) values (?, ?, ?, ?, ?, ?) on conflict do nothing",
                c.tenantId(), c.workspaceId(), c.scopeType(), c.scopeId(), currency, money(c.limitAmount()));
        return jdbc.query("select currency, limit_amount, spent_amount, reserved_amount, status, stop_reason from usage.spend_scope where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ? for update",
                rs -> rs.next() ? new SpendScope(rs.getString("currency"), rs.getBigDecimal("limit_amount"),
                        rs.getBigDecimal("spent_amount"), rs.getBigDecimal("reserved_amount"), rs.getString("status"),
                        rs.getString("stop_reason")) : null,
                c.tenantId(), c.workspaceId(), c.scopeType(), c.scopeId());
    }

    private SpendScope lockSpendScope(SpendReference ref) {
        return jdbc.query("select currency, limit_amount, spent_amount, reserved_amount, status, stop_reason from usage.spend_scope where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ? for update",
                rs -> rs.next() ? new SpendScope(rs.getString("currency"), rs.getBigDecimal("limit_amount"),
                        rs.getBigDecimal("spent_amount"), rs.getBigDecimal("reserved_amount"), rs.getString("status"),
                        rs.getString("stop_reason")) : null,
                ref.tenantId(), ref.workspaceId(), ref.scopeType(), ref.scopeId());
    }

    private SpendReservation denied(String code, BigDecimal amount, String currency) {
        // 拒绝原因只保存在本域审计/业务事实中，不作为 Metrics 标签，避免输入扩散为高基数。
        metrics.counter("eaf.usage.budget.denials").increment();
        return new SpendReservation(false, code, amount, currency);
    }

    private void stopScope(ReserveSpendCommand c, String reason) {
        jdbc.update("update usage.spend_scope set status = 'STOPPED', stop_reason = ?, updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ? and status = 'ACTIVE'",
                reason, c.tenantId(), c.workspaceId(), c.scopeType(), c.scopeId());
    }

    private BigDecimal reserveUpperBound(Price price, long maxTokens) {
        if ("REQUEST".equals(price.billingUnit())) return price.requestPrice().setScale(8, RoundingMode.CEILING);
        if (price.inputRate() == null || price.outputRate() == null) return null;
        var highestRate = price.inputRate().max(price.outputRate());
        return highestRate.multiply(BigDecimal.valueOf(maxTokens)).divide(MILLION, 8, RoundingMode.CEILING);
    }

    // 模型返回后把整笔上界预留转为费用；缺少可信用量时按全额占用，避免重启或超时导致重复出站。
    // 未获得可信计量或 Provider 响应时占用完整上界，避免未知成本被当成退款。
    private void settleReservedCall(UsageRecord observed, Estimate estimate) {
        var ref = spendReference(observed.callKey());
        if (ref == null) return;
        lockSpendScope(ref);
        var reservation = lockSpendReservation(observed.callKey());
        if (reservation == null || !"RESERVED".equals(reservation.state())) return;
        var currencyMismatch = observed.actualCost() != null && !Objects.equals(observed.actualCostCurrency(), reservation.currency());
        var charge = currencyMismatch ? reservation.reservedAmount() : observed.actualCost() != null ? money(observed.actualCost())
                : "SUCCEEDED".equals(observed.status()) && "ESTIMATED".equals(estimate.status())
                ? estimate.amount() : reservation.reservedAmount();
        var overrun = charge.compareTo(reservation.reservedAmount()) > 0;
        jdbc.update("update usage.spend_scope set reserved_amount = greatest(0, reserved_amount - ?), spent_amount = case when ? then greatest(limit_amount, spent_amount + ?) else spent_amount + ? end, status = case when ? or ? then 'STOPPED' else status end, stop_reason = case when ? then 'BILLING_CURRENCY_MISMATCH' when ? then 'SPEND_RESERVE_OVERRUN' else stop_reason end, updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ?",
                reservation.reservedAmount(), currencyMismatch, charge, charge, currencyMismatch, overrun,
                currencyMismatch, overrun, ref.tenantId(), ref.workspaceId(), ref.scopeType(), ref.scopeId());
        jdbc.update("update usage.spend_reservation set state = 'SETTLED', settled_amount = ?, billed_amount = ?, settled_at = now() where call_key = ? and state = 'RESERVED'",
                charge, observed.actualCost() == null ? null : money(observed.actualCost()), observed.callKey());
    }

    private SpendReference spendReference(String callKey) {
        return jdbc.query("select tenant_id, workspace_id, scope_type, scope_id from usage.spend_reservation where call_key = ?",
                rs -> rs.next() ? new SpendReference(rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getString("scope_type"),
                        rs.getObject("scope_id", UUID.class)) : null, callKey);
    }

    private SpendRow lockSpendReservation(String callKey) {
        return jdbc.query("select reserved_amount, settled_amount, billed_amount, currency, state from usage.spend_reservation where call_key = ? for update",
                rs -> rs.next() ? new SpendRow(rs.getBigDecimal("reserved_amount"), rs.getBigDecimal("settled_amount"),
                        rs.getBigDecimal("billed_amount"), rs.getString("currency"), rs.getString("state")) : null, callKey);
    }

    private void reconcileBilledSpend(String callKey, BigDecimal billedAmount, String currency) {
        var ref = spendReference(callKey);
        if (ref == null) return;
        var scope = lockSpendScope(ref);
        var reservation = lockSpendReservation(callKey);
        if (scope == null || reservation == null || !"SETTLED".equals(reservation.state())) return;
        if (!Objects.equals(scope.currency(), currency)) {
            jdbc.update("update usage.spend_scope set spent_amount = greatest(spent_amount, limit_amount), status = 'STOPPED', stop_reason = 'BILLING_CURRENCY_MISMATCH', updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ?",
                    ref.tenantId(), ref.workspaceId(), ref.scopeType(), ref.scopeId());
            jdbc.update("update usage.spend_reservation set billed_amount = ? where call_key = ?", billedAmount, callKey);
            return;
        }
        var delta = billedAmount.subtract(reservation.settledAmount());
        jdbc.update("update usage.spend_scope set spent_amount = greatest(0, spent_amount + ?), status = case when spent_amount + ? + reserved_amount > limit_amount then 'STOPPED' else status end, stop_reason = case when spent_amount + ? + reserved_amount > limit_amount then 'BILLED_COST_OVER_CAP' else stop_reason end, updated_at = now() where tenant_id = ? and workspace_id = ? and scope_type = ? and scope_id = ?",
                delta, delta, delta, ref.tenantId(), ref.workspaceId(), ref.scopeType(), ref.scopeId());
        jdbc.update("update usage.spend_reservation set settled_amount = ?, billed_amount = ? where call_key = ?",
                billedAmount, billedAmount, callKey);
    }

    private Estimate estimate(UsageRecord record, Price price) {
        if (price == null) return new Estimate(null, "UNKNOWN_PRICE");
        if ("REQUEST".equals(price.billingUnit())) {
            return "SUCCEEDED".equals(record.status())
                    ? new Estimate(price.requestPrice().setScale(8, RoundingMode.HALF_UP), "ESTIMATED")
                    : new Estimate(null, "UNKNOWN_USAGE");
        }
        var inputRequired = price.inputRate().signum() > 0;
        var outputRequired = price.outputRate().signum() > 0;
        if (!"KNOWN".equals(record.usageStatus()) || (inputRequired && record.inputTokens() == null)
                || (outputRequired && record.outputTokens() == null)) return new Estimate(null, "UNKNOWN_USAGE");
        var amount = BigDecimal.ZERO;
        if (record.inputTokens() != null) amount = amount.add(price.inputRate().multiply(BigDecimal.valueOf(record.inputTokens())).divide(MILLION, 8, RoundingMode.HALF_UP));
        if (record.outputTokens() != null) amount = amount.add(price.outputRate().multiply(BigDecimal.valueOf(record.outputTokens())).divide(MILLION, 8, RoundingMode.HALF_UP));
        return new Estimate(amount.setScale(8, RoundingMode.HALF_UP), "ESTIMATED");
    }

    private void validateRecord(UsageRecord record) {
        if (record == null || record.tenantId() == null || record.workspaceId() == null || record.callKey() == null
                || record.callKey().isBlank() || record.callKey().length() > 240 || record.callType() == null
                || record.scopeType() == null || record.scopeId() == null || record.startedAt() == null)
            throw EafException.invalid("Usage 记录必须绑定稳定调用键、作用域和开始时间。");
        if (record.inputTokens() != null && record.inputTokens() < 0 || record.outputTokens() != null && record.outputTokens() < 0
                || record.reservedTokens() < 0) throw EafException.invalid("Usage Token 数量不能为负数。");
    }

    private void validateBilling(BigDecimal amount, String currency, String source) {
        if (amount == null || amount.signum() < 0 || currency == null || !currency.matches("[A-Z]{3}")
                || source == null || source.isBlank() || source.length() > 120)
            throw EafException.invalid("账单事实必须包含非负金额、大写币种和可追溯来源。");
    }

    private boolean sameObservation(UsageRecord existing, UsageRecord request) {
        return Objects.equals(existing.tenantId(), request.tenantId()) && Objects.equals(existing.workspaceId(), request.workspaceId())
                && Objects.equals(existing.taskId(), request.taskId()) && Objects.equals(existing.runId(), request.runId())
                && existing.callNo() == request.callNo() && Objects.equals(existing.callType(), request.callType())
                && Objects.equals(existing.scopeType(), request.scopeType()) && Objects.equals(existing.scopeId(), request.scopeId())
                && Objects.equals(existing.source(), request.source()) && Objects.equals(existing.provider(), request.provider())
                && Objects.equals(existing.model(), request.model()) && Objects.equals(existing.inputTokens(), request.inputTokens())
                && Objects.equals(existing.outputTokens(), request.outputTokens()) && Objects.equals(existing.usageStatus(), request.usageStatus())
                && existing.reservedTokens() == request.reservedTokens() && Objects.equals(existing.status(), request.status());
    }

    private static UsageRecord map(ResultSet rs, int row) throws SQLException {
        return new UsageRecord(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("task_id", UUID.class), rs.getObject("run_id", UUID.class), rs.getString("source"),
                rs.getString("provider"), rs.getString("model"), (Integer) rs.getObject("input_tokens"),
                (Integer) rs.getObject("output_tokens"), rs.getString("usage_status"), rs.getBigDecimal("estimated_cost"),
                rs.getString("cost_currency"), rs.getString("cost_status"), rs.getString("cost_source"),
                rs.getInt("reserved_tokens"), rs.getString("status"), rs.getString("error_code"), instant(rs, "started_at"),
                instant(rs, "ended_at"), rs.getInt("call_no"), rs.getString("call_key"), rs.getString("call_type"),
                rs.getString("scope_type"), rs.getObject("scope_id", UUID.class), rs.getString("price_version"),
                rs.getString("price_source_version"), instant(rs, "price_effective_at"), rs.getString("billing_unit"),
                rs.getBigDecimal("input_price_per_million"), rs.getBigDecimal("output_price_per_million"),
                rs.getBigDecimal("request_price"), rs.getBigDecimal("actual_cost"), rs.getString("actual_cost_currency"),
                rs.getString("billing_source"));
    }

    private static Instant instant(ResultSet rs, String name) throws SQLException {
        var value = rs.getTimestamp(name);
        return value == null ? null : value.toInstant();
    }

    private static BigDecimal money(BigDecimal value) { return value.setScale(8, RoundingMode.HALF_UP); }

    // 跨事务复用 Scope 与费率快照，避免同一调用按新费率结算。
    private record SpendScope(String currency, BigDecimal limitAmount, BigDecimal spentAmount,
                              BigDecimal reservedAmount, String status, String stopReason) { }
    private record SpendReference(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId) { }
    private record SpendRow(BigDecimal reservedAmount, BigDecimal settledAmount, BigDecimal billedAmount,
                            String currency, String state) { }

    private record Price(String version, String source, String sourceVersion, String currency, String billingUnit,
                         BigDecimal inputRate, BigDecimal outputRate, BigDecimal requestPrice, Instant effectiveAt) { }
    private record Estimate(BigDecimal amount, String status) { }
}
