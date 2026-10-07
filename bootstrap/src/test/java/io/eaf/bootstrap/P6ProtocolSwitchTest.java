package io.eaf.bootstrap;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.agent-protocol.a2a-enabled=false", "eaf.agent-protocol.mcp-enabled=false",
        "eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 关闭协议入口后保留 Task/Execution REST 与恢复组件，验证 kill switch 的隔离范围。
class P6ProtocolSwitchTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String ALICE_TOKEN = "Bearer eaf-local-alice";
    private static final String WORKSPACE_ID = "10000000-0000-4000-8000-000000000001";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;

    @Test
    void protocolEntrancesCanBeDisabledWithoutRemovingTaskRest() throws Exception {
        // A2A Card 与 MCP servlet 返回 404，REST Task 路由仍由认证与业务校验处理。
        mvc.perform(get("/.well-known/agent-card.json").header("Authorization", ALICE_TOKEN))
                .andExpect(status().isNotFound());
        mvc.perform(post("/mcp").header("Authorization", ALICE_TOKEN).contentType(APPLICATION_JSON).content("{}"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks", WORKSPACE_ID)
                        .header("Authorization", ALICE_TOKEN).contentType(APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }
}
