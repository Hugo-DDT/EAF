package io.eaf.model.infrastructure;

import io.eaf.model.api.EvidenceAssessmentGateway;
import io.eaf.model.api.EvidenceAssessmentRequest;
import io.eaf.model.api.EvidenceAssessmentResult;
import io.eaf.model.api.EvidenceChoice;
import io.eaf.model.api.ModelFailure;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 固定演示素材的关键词夹具，不代表通用中文判断质量。 */
public final class DeterministicEvidenceAssessmentGateway implements EvidenceAssessmentGateway {
    private final AtomicInteger calls = new AtomicInteger();

    @Override public boolean enabled() { return true; }

    @Override
    public EvidenceAssessmentResult assess(EvidenceAssessmentRequest request) {
        if (request == null || request.question() == null || request.question().isBlank()
                || request.passages().isEmpty() || request.passages().size() > 10)
            throw new ModelFailure("EVIDENCE_INPUT_INVALID", "证据判断输入无效。", false, false);
        calls.incrementAndGet();
        var results = new LinkedHashMap<String, EvidenceChoice>();
        for (var passage : request.passages()) {
            var choice = classify(request.question(), passage.content());
            var probabilities = new LinkedHashMap<String, BigDecimal>();
            for (var name : List.of("ANSWERS", "CONTRADICTS", "RELATED", "IRRELEVANT"))
                probabilities.put(name, name.equals(choice) ? BigDecimal.ONE : BigDecimal.ZERO);
            results.put(passage.citationId(), new EvidenceChoice(choice, probabilities));
        }
        return new EvidenceAssessmentResult("deterministic", "p9-evidence-fixture-v1", results, 0, 0, "KNOWN");
    }

    private String classify(String question, String content) {
        var lowerQuestion = question.toLowerCase(java.util.Locale.ROOT);
        var lowerContent = content.toLowerCase(java.util.Locale.ROOT);
        if (lowerContent.contains("旧通知") && lowerQuestion.contains("三十天")) return "CONTRADICTS";
        if (lowerContent.contains("sku-cloud-renew-12") || lowerContent.contains("续约团队应在合同到期日前三十天"))
            return "ANSWERS";
        if (lowerContent.contains("complaint")) return "RELATED";
        return lowerContent.contains("云桥") || lowerContent.contains("续约") || lowerContent.contains("通知")
                ? "RELATED" : "IRRELEVANT";
    }

    @Override public int callCount() { return calls.get(); }
}
