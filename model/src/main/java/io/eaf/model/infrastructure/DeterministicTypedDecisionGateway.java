package io.eaf.model.infrastructure;

import io.eaf.model.api.TypedDecisionGateway;
import io.eaf.model.api.TypedDecisionRequest;
import io.eaf.model.api.TypedDecisionResult;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 本地演示替身只按固定关键词归类；不代表 Jev 的质量或业务事实。 */
final class DeterministicTypedDecisionGateway implements TypedDecisionGateway {
    private static final String[] CHOICES = {"LOW", "MEDIUM", "HIGH", "UNKNOWN"};
    private final AtomicInteger calls = new AtomicInteger();

    @Override public boolean enabled() { return true; }

    @Override
    public TypedDecisionResult decide(TypedDecisionRequest request) {
        if (request.state() == null || request.state().isBlank())
            throw new io.eaf.model.api.ModelFailure("DECISION_INPUT_INVALID", "分类摘要为空。", false, false);
        calls.incrementAndGet();
        var text = request.state().toLowerCase(Locale.ROOT);
        var choice = text.contains("信息不足") || text.contains("资料缺失") || text.contains("记录冲突")
                || text.contains("无法确认") ? "UNKNOWN"
                : text.contains("续约中断") || text.contains("重大投诉") || text.contains("明确高风险") ? "HIGH"
                : text.contains("续约") || text.contains("满意") || text.contains("稳定") ? "LOW" : "MEDIUM";
        var probabilities = new LinkedHashMap<String, BigDecimal>();
        for (var candidate : CHOICES) probabilities.put(candidate, candidate.equals(choice) ? BigDecimal.ONE : BigDecimal.ZERO);
        return new TypedDecisionResult("deterministic", "risk-choice-v1", choice, probabilities,
                null, null, "NOT_APPLICABLE");
    }

    @Override public int callCount() { return calls.get(); }
}
