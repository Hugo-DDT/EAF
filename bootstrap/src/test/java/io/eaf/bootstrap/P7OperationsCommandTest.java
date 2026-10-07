package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.connector.api.ConnectorService;
import io.eaf.task.api.TaskService;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 使用 PostgreSQL 验证运维命令的授权、幂等冲突和新 Task 闸门。 */
@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false", "eaf.execution.remote-poller-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false", "eaf.security.mode=disabled"})
@AutoConfigureMockMvc
class P7OperationsCommandTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TEST_CRM_CONNECTOR = UUID.fromString("23000000-0000-4000-8000-000000000001");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @Autowired private MockMvc mvc;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper json;
    @Autowired private TaskService tasks;
    @Autowired private ConnectorService connectors;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @BeforeEach
    void prepareOperatorAndResetGate() {
        jdbc.update("update \"identity\".subject set status = 'ACTIVE' where id in (?, ?)", Ids.ALICE, Ids.BOB);
        jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.BOB);
        jdbc.update("delete from workspace.\"grant\" where workspace_id = ? and actor_id = ?",
                Ids.WORKSPACE_A, Ids.BOB);
        grant(Ids.BOB, "workspace:operations:stop");
        grant(Ids.BOB, "workspace:operations:resume");
        grant(Ids.BOB, "connector:disable");
        jdbc.update("update connector.instance set status = 'ACTIVE', row_version = row_version + 1 "
                        + "where id = ? and tenant_id = ? and workspace_id = ?",
                TEST_CRM_CONNECTOR, Ids.TENANT_A, Ids.WORKSPACE_A);
        jdbc.update("update workspace.operational_gate set enabled = true, changed_by = null, command_id = null "
                        + "where tenant_id = ? and workspace_id = ? and gate_name in ('TASK_ADMISSION', 'BUSINESS_OUTBOUND')",
                Ids.TENANT_A, Ids.WORKSPACE_A);
    }

    @Test
    void connectorDisableIsOwnerAuthorizedScopedAndIdempotent() throws Exception {
        var versionBefore = jdbc.queryForObject("select row_version from connector.instance where id = ?",
                Long.class, TEST_CRM_CONNECTOR);
        var requestKey = "p7-ops-connector-disable-" + UUID.randomUUID();
        var url = "/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/connectors/"
                + TEST_CRM_CONNECTOR + "/disable";
        mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"CRM 凭据轮换窗口停用\"}")
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isForbidden());

        var response = mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"CRM 凭据轮换窗口停用\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("DISABLED"))
                .andExpect(jsonPath("$.version").value(versionBefore + 1))
                .andExpect(jsonPath("$.replayed").value(false))
                .andReturn().getResponse().getContentAsString();
        var commandId = json.readTree(response).path("commandId").asText();
        assertThat(jdbc.queryForObject("select status from connector.instance where id = ?", String.class, TEST_CRM_CONNECTOR))
                .isEqualTo("DISABLED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "connector-ops:" + commandId)).isEqualTo(1);

        mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"CRM 凭据轮换窗口停用\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.replayed").value(true))
                .andExpect(jsonPath("$.commandId").value(commandId));
        mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"换一个不同原因\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("CONNECTOR_COMMAND_CONFLICT"));
        assertThatThrownBy(() -> connectors.requireActive(Ids.TENANT_A, Ids.WORKSPACE_A, "TEST_CRM"))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("CONNECTOR_UNAVAILABLE"));
    }

    @Test
    void businessOutboundGateIsIndependentFromTaskAdmission() throws Exception {
        var priorCommandCount = jdbc.queryForObject("select count(*) from workspace.operations_command where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A);
        var url = "/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/gates/business-outbound";
        mvc.perform(put(url).header("Idempotency-Key", "p7-ops-outbound-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false,\"reason\":\"外发暂停演练\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(false));

        assertThat(jdbc.queryForObject("select enabled from workspace.operational_gate where tenant_id = ? and workspace_id = ? and gate_name = 'TASK_ADMISSION'",
                Boolean.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isTrue();
        assertThat(jdbc.queryForObject("select enabled from workspace.operational_gate where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Boolean.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isFalse();
        assertThat(jdbc.queryForObject("select count(*) from workspace.operations_command where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isEqualTo(priorCommandCount + 1);
    }

    @Test
    void stopCommandIsAuthorizedIdempotentAndBlocksCreationAndClaimUntilReopened() throws Exception {
        var priorCommandCount = jdbc.queryForObject("select count(*) from workspace.operations_command where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A);
        var taskRequestKey = "p7-ops-existing-" + UUID.randomUUID();
        var queuedTaskId = createTask("queued before operator stop", taskRequestKey);
        var requestKey = "p7-ops-stop-" + UUID.randomUUID();
        var url = "/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/gates/task-admission";

        mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false,\"reason\":\"试点停机演练\"}")
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isForbidden());

        var accepted = mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false,\"reason\":\"试点停机演练\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(false))
                .andExpect(jsonPath("$.replayed").value(false))
                .andReturn().getResponse().getContentAsString();
        var commandId = json.readTree(accepted).path("commandId").asText();
        var version = json.readTree(accepted).path("version").asLong();
        var auditCount = jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?",
                Integer.class, "workspace-ops:" + commandId);
        assertThat(auditCount).isEqualTo(1);

        var replay = mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false,\"reason\":\"试点停机演练\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.replayed").value(true))
                .andExpect(jsonPath("$.commandId").value(commandId))
                .andExpect(jsonPath("$.version").value(version))
                .andReturn().getResponse().getContentAsString();
        assertThat(replay).doesNotContain("试点停机演练");
        assertThat(jdbc.queryForObject("select count(*) from workspace.operations_command where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isEqualTo(priorCommandCount + 1);

        mvc.perform(put(url).header("Idempotency-Key", requestKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false,\"reason\":\"不同原因\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("OPERATIONS_COMMAND_CONFLICT"));

        assertThat(jdbc.queryForObject("select status from task.task where id = ?", String.class, queuedTaskId))
                .isEqualTo("QUEUED");
        assertThat(jdbc.queryForObject("select count(*) from task.task where id = ?", Integer.class, queuedTaskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from task.task where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isEqualTo(1);

        mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", taskRequestKey)
                        .contentType(MediaType.APPLICATION_JSON).content(taskBody("queued before operator stop"))
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.id").value(queuedTaskId.toString()));
        mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", taskRequestKey)
                        .contentType(MediaType.APPLICATION_JSON).content(taskBody("different content"))
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));
        assertThat(jdbc.queryForObject("select count(*) from task.task where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isEqualTo(1);

        mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", "p7-ops-blocked-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(taskBody("must remain blocked"))
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("WORKSPACE_TASK_ADMISSION_STOPPED"));
        assertThat(jdbc.queryForObject("select count(*) from task.task where tenant_id = ? and workspace_id = ?",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isEqualTo(1);
        assertThat(tasks.claimOne()).isEmpty();

        var reopenKey = "p7-ops-reopen-" + UUID.randomUUID();
        mvc.perform(put(url).header("Idempotency-Key", reopenKey).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":true,\"reason\":\"停机演练结束\"}")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(true));
        mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", "p7-ops-resumed-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(taskBody("accepted after reopen"))
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isAccepted());
        var lateWorker = tasks.claimOne().orElseThrow();
        assertThat(lateWorker.id()).isEqualTo(queuedTaskId);

        // 取消先撤销租约 fence；旧 Worker 晚到的完成回调不能覆盖终态或写入结果。
        var alice = new io.eaf.shared.ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
        var beforeCancel = tasks.get(alice, Ids.WORKSPACE_A, queuedTaskId);
        var cancelled = tasks.cancel(alice, Ids.WORKSPACE_A, queuedTaskId, beforeCancel.version());
        assertThat(cancelled.status().name()).isEqualTo("CANCELLED");
        var completedFactsBeforeLateArrival = jdbc.queryForObject("select count(*) from audit.audit_event "
                        + "where tenant_id = ? and task_id = ? and action = 'TASK_COMPLETED'",
                Integer.class, Ids.TENANT_A, queuedTaskId);

        tasks.complete(lateWorker, io.eaf.task.api.TaskRunner.RunOutcome.success("{\"late\":true}", false, null, null));

        var afterLateArrival = tasks.get(alice, Ids.WORKSPACE_A, queuedTaskId);
        assertThat(afterLateArrival.status().name()).isEqualTo("CANCELLED");
        assertThat(afterLateArrival.resultJson()).isNull();
        assertThat(jdbc.queryForObject("select status from task.task_attempt where task_id = ? and attempt = 1",
                String.class, queuedTaskId)).isEqualTo("CANCELLED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event "
                        + "where tenant_id = ? and task_id = ? and action = 'TASK_COMPLETED'",
                Integer.class, Ids.TENANT_A, queuedTaskId)).isEqualTo(completedFactsBeforeLateArrival);

        // 清理由恢复闸门创建的第二个新 Task，避免该测试留下可被其他测试领取的队列项。
        var resumedWorker = tasks.claimOne().orElseThrow();
        var resumed = tasks.get(alice, Ids.WORKSPACE_A, resumedWorker.id());
        tasks.cancel(alice, Ids.WORKSPACE_A, resumedWorker.id(), resumed.version());
    }

    private UUID createTask(String input, String requestKey) throws Exception {
        var response = mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", requestKey)
                        .contentType(MediaType.APPLICATION_JSON).content(taskBody(input))
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        return UUID.fromString(json.readTree(response).path("id").asText());
    }

    private static String taskBody(String input) {
        return "{\"agentId\":\"" + Ids.AGENT_RISK + "\",\"agentVersion\":\"1.1.0\",\"input\":\"" + input + "\"}";
    }

    private void grant(UUID subjectId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, subjectId, action);
    }

    private static UsernamePasswordAuthenticationToken principalAuthentication(UUID subjectId) {
        return new UsernamePasswordAuthenticationToken(new ActorContext(subjectId, Ids.TENANT_A,
                ActorType.HUMAN, Set.of()), null, List.of());
    }
}
