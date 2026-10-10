package io.eaf.workflow.infrastructure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.EafException;
import org.junit.jupiter.api.Test;

class WorkflowSchemasTest {
    private final WorkflowSchemas schemas = new WorkflowSchemas(new ObjectMapper());
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void strictWorkflowSchemaRejectsUnsupportedFieldsAndUnknownInput() throws Exception {
        var schema = "{\"type\":\"object\",\"required\":[\"name\"],\"additionalProperties\":false,"
                + "\"properties\":{\"name\":{\"type\":\"string\",\"maxLength\":8}}}";
        schemas.parseWorkflowSchema(schema, "inputSchema");
        assertThatThrownBy(() -> schemas.parseWorkflowSchema(schema.replace("\"type\":\"string\"", 
                "\"type\":\"string\",\"pattern\":\".*\""), "inputSchema"))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> schemas.validateInput(schema, json.readTree("{\"name\":\"ok\",\"extra\":1}")))
                .isInstanceOf(EafException.class);
    }

    @Test
    void legacyRequiredOnlyOutputAndArrayItemTypesRemainValidated() throws Exception {
        schemas.validateDeclaredOutput("{\"type\":\"object\",\"required\":[\"status\"],"
                + "\"additionalProperties\":false}", json.readTree("{\"status\":\"OK\"}"));
        assertThatThrownBy(() -> schemas.validateDeclaredOutput("{\"type\":\"object\",\"required\":[\"status\"],"
                + "\"additionalProperties\":false}", json.readTree("{\"status\":\"OK\",\"extra\":1}")))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> schemas.validateDeclaredOutput("{\"type\":\"object\",\"required\":[\"items\"],"
                + "\"additionalProperties\":false,\"properties\":{\"items\":{\"type\":\"array\","
                + "\"items\":{\"type\":\"string\"}}}}", json.readTree("{\"items\":[1]}")))
                .isInstanceOf(EafException.class);
    }
}
