package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 在隔离 PostgreSQL 中验证 Capability 选择、Task 快照、冲突拒绝和撤回后无模型调用。
class P5CapabilityTaskTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired CapabilityService capabilities;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;
    @Autowired MockMvc mvc;

    @Test
    void tasksKeepSelectedVersionsAndStopBeforeRuntimeAfterCapabilityWithdrawal() throws Exception {
        var conflictCount = jdbc.queryForObject("select count(*) from task.task", Integer.class);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-capability-conflict")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"agentId":"20000000-0000-4000-8000-000000000001","agentVersion":"1.1.0",
                                 "capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0",
                                 "input":"检查客户风险"}
                                """))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("CAPABILITY_AGENT_CONFLICT"));
        assertThat(jdbc.queryForObject("select count(*) from task.task", Integer.class)).isEqualTo(conflictCount);

        var original = capabilities.requirePublished(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.0.0");
        var httpTask = mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-capability-http-create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"capabilityId":"54000000-0000-4000-8000-000000000001","capabilityVersion":"1.0.0",
                                 "input":"检查客户风险"}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.assetBinding.capabilityVersion").value("1.0.0"))
                .andReturn();
        var firstId = UUID.fromString(json.readTree(httpTask.getResponse().getContentAsString()).path("id").asText());
        var first = tasks.get(ALICE, Ids.WORKSPACE_A, firstId);
        var originalBinding = binding(original);
        assertThat(first.assetBinding()).isEqualTo(originalBinding);

        var second = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, original.agentId(), original.agentVersion(),
                "另一个任务", null, null, "p5-capability-second", "trace-capability-second", "USER", originalBinding));
        assertThat(second.assetBinding()).isEqualTo(originalBinding);

        //  占用至 1.4.0， 使用 1.5.0；本用例用 1.6.0 验证任务快照不变。
        var nextDraft = capabilities.addVersion(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID,
                new CreateCapabilityVersionCommand("1.6.0", original.agentId(), original.agentVersion(),
                        original.skillId(), original.skillVersion(), original.promptId(), original.promptVersion(),
                        original.toolDependencies().stream().map(tool -> new CapabilityToolReference(tool.name(), tool.version())).toList(),
                        original.evaluationRef()));
        var nextPublished = capabilities.publish(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.6.0", nextDraft.rowVersion());
        var next = capabilities.requirePublished(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.6.0");
        assertThat(nextPublished.status()).isEqualTo("PUBLISHED");
        var third = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, next.agentId(), next.agentVersion(),
                "新版本任务", null, null, "p5-capability-third", "trace-capability-third", "USER", binding(next)));
        assertThat(third.assetBinding().capabilityVersion()).isEqualTo("1.6.0");
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, firstId).assetBinding()).isEqualTo(originalBinding);

        capabilities.revoke(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.0.0", original.rowVersion());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(firstId);
        var outcome = runtime.run(work);
        tasks.complete(work, outcome);
        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.modelCalled()).isFalse();
        assertThat(outcome.modelCalls()).isZero();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, firstId).assetBinding()).isEqualTo(originalBinding);
    }

    private TaskAssetBinding binding(io.eaf.capability.api.CapabilityDefinition capability) {
        return new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
    }
}
// 本文件负责实现 CapabilityTaskTest.java 相关代码。
