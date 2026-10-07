package io.eaf.agentruntime.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.model.api.ModelMessage;
import io.eaf.shared.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** 仅改变知识 DTO 的模型呈现，不改变用于授权和引用校验的完整快照。 */
final class ServiceRequestPresentation {
    private ServiceRequestPresentation() { }

    static ObjectNode compactKnowledge(ObjectMapper json, EnterpriseContext context) {
        if (context == null || context.items() == null) throw new IllegalArgumentException("服务请求知识快照无效。");
        var compact = json.createObjectNode();
        if (context.status() != null) compact.put("status", context.status());
        compact.put("omittedCount", context.omittedCount());
        if (context.truncationReason() != null) compact.put("truncationReason", context.truncationReason());
        var items = compact.putArray("items");
        for (var item : context.items()) {
            if (item == null || item.sourceType() != null && !"KNOWLEDGE".equals(item.sourceType()))
                throw new IllegalArgumentException("只接受正式 Knowledge 项。");
            var source = json.valueToTree(item);
            if (!source.hasNonNull("citationId") || !source.hasNonNull("content"))
                throw new IllegalArgumentException("Knowledge 正文或引用标识缺失。");
            var target = items.addObject();
            target.put("citationId", source.path("citationId").asText());
            target.put("sourceType", "KNOWLEDGE");
            target.set("content", source.path("content").deepCopy());
            for (var field : List.of("headingPath", "sourceRef",
                    "documentVersion", "scope", "evidenceRefs", "expiresAt", "businessEntityType", "businessEntityId")) {
                JsonNode value = source.get(field);
                if (value == null || value.isNull() || value.isArray() && value.isEmpty()
                        || value.isTextual() && value.asText().isEmpty()) continue;
                target.set(field, value.deepCopy());
            }
        }
        return compact;
    }

    static int chars(String value) {
        return value.codePointCount(0, value.length());
    }

    static int utf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    static String messageHash(ObjectMapper json, List<ModelMessage> messages) {
        ArrayNode sequence = json.createArrayNode();
        for (var message : messages) {
            var entry = sequence.addObject();
            entry.put("role", message.role());
            entry.put("content", message.content());
        }
        try { return Hashing.sha256(json.writeValueAsString(sequence)); }
        catch (Exception invalid) { throw new IllegalStateException("服务请求消息摘要无法生成。", invalid); }
    }
}
