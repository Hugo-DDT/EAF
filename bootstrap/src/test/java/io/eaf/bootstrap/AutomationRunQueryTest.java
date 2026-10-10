package io.eaf.bootstrap;

import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.workflow.api.WorkflowAutomationService;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.ArrayList;
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

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.workflow.automation.enabled=false"})
class AutomationRunQueryTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
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

    @Autowired WorkflowAutomationService automations;
    @Autowired JdbcTemplate jdbc;

    @Test
    void emptyAndPagedRunQueriesWorkInReadOnlyTransactionsWithoutWrites() {
        var subscriptionId = UUID.randomUUID();
        seedSubscription(subscriptionId);
        var before = jdbc.queryForObject("select (select count(*) from workflow.automation_run) + "
                + "(select count(*) from task.task)", Long.class);

        assertThat(automations.listRuns(ALICE, Ids.WORKSPACE_A, subscriptionId, null, null, 5).items()).isEmpty();

        var ids = new ArrayList<UUID>();
        for (var index = 0; index < 3; index++) {
            var id = UUID.randomUUID();
            ids.add(id);
            jdbc.update("insert into workflow.automation_run(id, tenant_id, workspace_id, owner_id, subscription_id, "
                            + "authorization_epoch, trigger_kind, trigger_key, planned_at, status, created_at) "
                            + "values (?, ?, ?, ?, ?, 1, 'WEEKLY', ?, ?, 'SKIPPED', ?)",
                    id, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, subscriptionId, "query-test-" + index,
                    Timestamp.from(Instant.now().minusSeconds(index + 1)),
                    Timestamp.from(Instant.now().minusSeconds(index + 1)));
        }

        var first = automations.listRuns(ALICE, Ids.WORKSPACE_A, subscriptionId, null, null, 2);
        var second = automations.listRuns(ALICE, Ids.WORKSPACE_A, subscriptionId,
                first.nextCreatedAt(), first.nextId(), 2);

        assertThat(first.items()).hasSize(2);
        assertThat(first.nextId()).isNotNull();
        assertThat(second.items()).hasSize(1);
        assertThat(second.nextId()).isNull();
        assertThat(java.util.stream.Stream.concat(first.items().stream(), second.items().stream())
                .map(run -> run.id()).toList()).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(jdbc.queryForObject("select (select count(*) from workflow.automation_run) + "
                + "(select count(*) from task.task)", Long.class)).isEqualTo(before + 3);
    }

    private void seedSubscription(UUID id) {
        jdbc.update("insert into workflow.automation_subscription(id, tenant_id, workspace_id, owner_id, name, trigger_kind, "
                        + "day_of_week, local_time, time_zone, max_items, expires_at, max_runs, status, request_hash, "
                        + "agent_id, agent_version, capability_id, capability_version, capability_hash, skill_id, "
                        + "skill_version, skill_hash, profile_hash) values (?, ?, ?, ?, 'query fixture', 'WEEKLY', 'MONDAY', "
                        + "'09:00', 'UTC', 1, now() + interval '1 day', 3, 'PAUSED', ?, ?, '1.0.0', ?, '1.0.0', ?, ?, "
                        + "'1.0.0', ?, ?)", id, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, "a".repeat(64),
                UUID.fromString("20000000-0000-4000-8000-000000000024"),
                UUID.fromString("54000000-0000-4000-8000-000000000024"), "b".repeat(64),
                UUID.fromString("53000000-0000-4000-8000-000000000024"), "c".repeat(64), "d".repeat(64));
    }
}
