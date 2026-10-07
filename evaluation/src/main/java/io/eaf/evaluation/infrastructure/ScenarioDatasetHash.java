package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.Hashing;
import java.util.ArrayList;
import java.util.List;

/** 用稳定字段顺序校验不可变场景标签和整套数据版本。 */
final class ScenarioDatasetHash {
    private static final String SEPARATOR = "\u001f";

    private ScenarioDatasetHash() { }

    static String answer(ObjectMapper json, String category, String outcome, String evidenceRefsJson) {
        try {
            var refs = json.readTree(evidenceRefsJson);
            if (!refs.isArray() || outcome == null) return "";
            return Hashing.sha256(String.join(SEPARATOR, String.valueOf(category), outcome,
                    json.writeValueAsString(refs)));
        } catch (java.io.IOException invalid) { return ""; }
    }

    static String dataset(String key, String version, String scoringVersion, List<Entry> entries) {
        var fields = new ArrayList<String>();
        fields.add(key); fields.add(version); fields.add(scoringVersion);
        for (var entry : entries) {
            fields.add(entry.caseId()); fields.add(entry.split()); fields.add(entry.inputHash()); fields.add(entry.answerHash());
        }
        return Hashing.sha256(String.join(SEPARATOR, fields));
    }

    record Entry(String caseId, String split, String inputHash, String answerHash) { }
}
