package io.eaf.workflow.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import java.util.HashSet;
import java.util.Set;

/** Workflow 的纯 Schema 解析与值校验；严格定义和旧 Tool/Skill 输出保持不同规则。 */
final class WorkflowSchemas {
    private static final int MAX_SCHEMA_LENGTH = 16_384;
    private static final Set<String> WORKFLOW_SCHEMA_FIELDS = Set.of("type", "required", "additionalProperties", "properties");
    private static final Set<String> WORKFLOW_PROPERTY_FIELDS = Set.of("type", "enum", "minLength", "maxLength");
    private final ObjectMapper json;

    WorkflowSchemas(ObjectMapper json) { this.json = json; }

    void validateInput(String schemaText, JsonNode input) {
        var schema = parseWorkflowSchema(schemaText, "inputSchema");
        if (!input.isObject()) throw EafException.invalid("Workflow input 必须是 JSON object。");
        var properties = schema.path("properties");
        if (!schema.path("additionalProperties").asBoolean(false))
            input.fieldNames().forEachRemaining(field -> {
                if (!properties.has(field)) throw EafException.invalid("Workflow input 含有未声明字段：" + field);
            });
        var required = schema.path("required");
        if (required.isArray()) for (var field : required)
            if (!input.has(field.asText())) throw EafException.invalid("Workflow input 缺少必填字段：" + field.asText());
        properties.fields().forEachRemaining(entry -> {
            if (!input.has(entry.getKey())) return;
            var value = input.get(entry.getKey());
            var property = entry.getValue();
            var type = property.path("type").asText();
            if (!matchesType(value, type)) throw EafException.invalid("Workflow input 字段类型无效：" + entry.getKey());
            if (value.isTextual() && property.has("minLength") && value.asText().length() < property.path("minLength").asInt())
                throw EafException.invalid("Workflow input 字段长度不足：" + entry.getKey());
            if (value.isTextual() && property.has("maxLength") && value.asText().length() > property.path("maxLength").asInt())
                throw EafException.invalid("Workflow input 字段长度超限：" + entry.getKey());
            if (property.path("enum").isArray() && !enumContains(property.path("enum"), value))
                throw EafException.invalid("Workflow input 字段值不在允许范围：" + entry.getKey());
        });
    }

    void validateDeclaredOutput(String schemaText, JsonNode output) {
        var schema = parseObjectSchema(schemaText, "step.outputSchema");
        if (output == null || !output.isObject()) throw EafException.invalid("Workflow 子 Task 输出必须是 JSON object。");
        var properties = schema.path("properties");
        var required = schema.path("required");
        var propertiesDeclared = properties.isObject() && properties.size() > 0;
        var requiredNames = new HashSet<String>();
        if (required.isArray()) required.forEach(field -> requiredNames.add(field.asText()));
        if (!schema.path("additionalProperties").asBoolean(false))
            output.fieldNames().forEachRemaining(field -> {
                // 旧 Tool/Skill Schema 仅声明 required 时，保留其已发布的输出兼容规则。
                if (!properties.has(field) && !(!propertiesDeclared && requiredNames.contains(field)))
                    throw EafException.invalid("Workflow 子 Task 输出含有未声明字段：" + field);
            });
        if (required.isArray()) for (var field : required)
            if (!output.has(field.asText())) throw EafException.invalid("Workflow 子 Task 输出缺少字段：" + field.asText());
        properties.fields().forEachRemaining(entry -> {
            if (!output.has(entry.getKey())) return;
            var value = output.get(entry.getKey());
            var property = entry.getValue();
            var type = property.path("type").asText();
            if (!matchesType(value, type)) throw EafException.invalid("Workflow 子 Task 输出字段类型无效：" + entry.getKey());
            if (property.path("enum").isArray() && !enumContains(property.path("enum"), value))
                throw EafException.invalid("Workflow 子 Task 输出字段值不在允许范围：" + entry.getKey());
            if (value.isTextual() && property.has("minLength") && value.asText().length() < property.path("minLength").asInt())
                throw EafException.invalid("Workflow 子 Task 输出字段长度不足：" + entry.getKey());
            if (value.isTextual() && property.has("maxLength") && value.asText().length() > property.path("maxLength").asInt())
                throw EafException.invalid("Workflow 子 Task 输出字段长度超限：" + entry.getKey());
            if (value.isArray() && property.path("items").has("type")) {
                var itemType = property.path("items").path("type").asText();
                for (var item : value) if (!matchesType(item, itemType))
                    throw EafException.invalid("Workflow 子 Task 输出数组元素类型无效：" + entry.getKey());
            }
        });
    }

    JsonNode parseWorkflowSchema(String source, String field) {
        var schema = parseObjectSchema(source, field);
        if (!schema.path("additionalProperties").isBoolean() || schema.path("additionalProperties").asBoolean())
            throw EafException.invalid(field + " 必须显式设置 additionalProperties=false。");
        if (!hasOnlyFields(schema, WORKFLOW_SCHEMA_FIELDS) || !schema.path("properties").isObject())
            throw EafException.invalid(field + " 包含不支持的 Schema 字段或缺少 properties 对象。");
        var required = schema.path("required");
        if (!required.isArray()) throw EafException.invalid(field + " 必须声明 required 数组。");
        var seen = new HashSet<String>();
        for (var property : required) {
            if (!property.isTextual() || !schema.path("properties").has(property.asText()) || !seen.add(property.asText()))
                throw EafException.invalid(field + " required 字段无效。");
        }
        schema.path("properties").fields().forEachRemaining(entry -> {
            var property = entry.getValue();
            var type = property.path("type").asText();
            if (!property.isObject() || !hasOnlyFields(property, WORKFLOW_PROPERTY_FIELDS)
                    || !Set.of("string", "integer", "number", "boolean", "object", "array").contains(type))
                throw EafException.invalid(field + " 包含不支持的属性 Schema：" + entry.getKey());
            if (property.has("enum")) {
                var values = property.path("enum");
                if (!values.isArray() || values.size() == 0) throw EafException.invalid(field + " enum 必须是非空数组。");
                for (var value : values) if (!matchesType(value, type))
                    throw EafException.invalid(field + " enum 值类型无效：" + entry.getKey());
            }
            var min = property.path("minLength");
            var max = property.path("maxLength");
            if (property.has("minLength") || property.has("maxLength")) {
                if (!"string".equals(type) || property.has("minLength") && (!min.isIntegralNumber() || min.asInt() < 0)
                        || property.has("maxLength") && (!max.isIntegralNumber() || max.asInt() < 0)
                        || property.has("minLength") && property.has("maxLength") && min.asInt() > max.asInt())
                    throw EafException.invalid(field + " 字符串长度约束无效：" + entry.getKey());
            }
        });
        return schema;
    }

    JsonNode parseObjectSchema(String source, String field) {
        if (source == null || source.length() > MAX_SCHEMA_LENGTH) throw EafException.invalid(field + " 为空或超过长度限制。");
        var schema = parseJson(source, field);
        if (!schema.isObject() || !"object".equals(schema.path("type").asText())
                || schema.has("properties") && !schema.path("properties").isObject())
            throw EafException.invalid(field + " 必须是 JSON object Schema。");
        schema.path("properties").fields().forEachRemaining(entry -> {
            var type = entry.getValue().path("type").asText();
            if (!Set.of("string", "integer", "number", "boolean", "object", "array").contains(type))
                throw EafException.invalid(field + " 包含不支持的属性类型：" + entry.getKey());
        });
        return schema;
    }

    private JsonNode parseJson(String source, String field) {
        try {
            var result = source == null ? null : json.readTree(source);
            if (result == null) throw EafException.invalid(field + " 必须是有效 JSON。");
            return result;
        } catch (JsonProcessingException e) {
            throw EafException.invalid(field + " 必须是有效 JSON。");
        }
    }

    private boolean hasOnlyFields(JsonNode node, Set<String> allowed) {
        var fields = node.fieldNames();
        while (fields.hasNext()) if (!allowed.contains(fields.next())) return false;
        return true;
    }

    private boolean matchesType(JsonNode value, String type) {
        return switch (type) {
            case "string" -> value.isTextual();
            case "integer" -> value.isIntegralNumber();
            case "number" -> value.isNumber();
            case "boolean" -> value.isBoolean();
            case "object" -> value.isObject();
            case "array" -> value.isArray();
            default -> false;
        };
    }

    private boolean enumContains(JsonNode values, JsonNode value) {
        for (var allowed : values) if (allowed.equals(value)) return true;
        return false;
    }
}
