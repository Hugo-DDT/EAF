package io.eaf.bootstrap;

import io.eaf.evaluation.api.ScenarioEvaluationService;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.QualityRunSourceVerifier;
import io.eaf.task.api.TaskService;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.evaluation.scenario-poll-delay-ms=3600000"})
class TaskOwnerBoundaryTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID P15_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired TaskService tasks;
    @Autowired KnowledgeService knowledge;
    @Autowired QualityRunSourceVerifier qualityRuns;
    @Autowired ScenarioEvaluationService scenarios;
    @Autowired JdbcTemplate jdbc;

    @Test
    void taskDeadlineAndScenarioOwnerStayInsideTheirExactScopes() {
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "synthetic deadline boundary", null, null, "deadline-boundary-" + UUID.randomUUID(),
                "task-owner-boundary", "USER"));
        assertThat(tasks.deadlineAt(Ids.TENANT_A, Ids.WORKSPACE_A, task.id())).isAfter(Instant.now());
        assertNotFound(() -> tasks.deadlineAt(Ids.TENANT_B, Ids.WORKSPACE_A, task.id()));
        assertNotFound(() -> tasks.deadlineAt(Ids.TENANT_A, Ids.WORKSPACE_A2, task.id()));

        var knowledgeDoc = knowledge.create(new CreateKnowledgeDocumentCommand(ALICE, Ids.WORKSPACE_A,
                "合成来源边界", "manual://task-owner-boundary", "设备故障处理的合成测试依据。",
                java.util.Map.of("synthetic", "true"), "task-owner-knowledge", "task-owner-boundary"));
        knowledge.chunk(ALICE, Ids.WORKSPACE_A, knowledgeDoc.id(), "p3-plain-1");
        var build = knowledge.buildIndex(ALICE, Ids.WORKSPACE_A, knowledgeDoc.id(), "p3-plain-1",
                "task-owner-knowledge-index");
        knowledge.publish(ALICE, Ids.WORKSPACE_A, knowledgeDoc.id(), knowledgeDoc.rowVersion(), build.id(),
                "task-owner-knowledge-publish");

        var run = scenarios.createRun(ALICE, Ids.WORKSPACE_A,
                new ScenarioEvaluationService.ScenarioRunRequest("service-request-synthetic", "1.0.0", "HELD_OUT",
                        "SINGLE", new ScenarioEvaluationService.CapabilityVersion(P15_CAPABILITY, "1.0.0"), null,
                        Instant.now().plusSeconds(600)), "task-owner-boundary-" + UUID.randomUUID()).report();
        scenarios.dispatchNext();
        var sampleTaskId = scenarios.listSamples(ALICE, Ids.WORKSPACE_A, run.runId(), 40).stream()
                .map(ScenarioEvaluationService.ScenarioSampleView::taskId).filter(java.util.Objects::nonNull)
                .findFirst().orElseThrow();

        assertThat(qualityRuns.scenarioTaskOwner(Ids.TENANT_A, Ids.WORKSPACE_A, sampleTaskId))
                .contains(Ids.ALICE);
        assertThat(qualityRuns.scenarioTaskOwner(Ids.TENANT_B, Ids.WORKSPACE_A, sampleTaskId)).isEmpty();
        assertThat(qualityRuns.scenarioTaskOwner(Ids.TENANT_A, Ids.WORKSPACE_A2, sampleTaskId)).isEmpty();
        tasks.getScenarioEvaluationTask(ALICE, Ids.WORKSPACE_A, sampleTaskId);

        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'evaluation:read', 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                        + "do update set status = 'ACTIVE', tenant_id = excluded.tenant_id",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());
        assertNotFound(() -> tasks.getScenarioEvaluationTask(bob, Ids.WORKSPACE_A, sampleTaskId));
    }

    private void assertNotFound(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(EafException.class,
                error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
    }
}
