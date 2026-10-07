package io.eaf.memory.api;

import java.time.Instant;
import java.util.List;

// 新版本独立保存内容、范围、来源与有效期；续期也必须产生新版本。
public record CreateMemoryVersionCommand(String version, String type, String scope, String content,
                                         Double confidence, Instant expiresAt, String sourceRef,
                                         List<String> evidenceRefs, String businessEntityType,
                                         String businessEntityId) {
    public CreateMemoryVersionCommand(String version, String type, String scope, String content, Double confidence,
                                      Instant expiresAt, String sourceRef, List<String> evidenceRefs) {
        this(version, type, scope, content, confidence, expiresAt, sourceRef, evidenceRefs, null, null);
    }
}
