package io.eaf.bootstrap;

import io.eaf.audit.api.AuditPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceOperationalControl;
import io.eaf.workspace.infrastructure.JdbcWorkspaceOperationalControl;
import java.nio.file.Files;
import java.time.Clock;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PostgreSQL 备份恢复后验证恢复 Profile 屏障，并保留原授权事实等待人工对账。 */
@Testcontainers
@ActiveProfiles("recovery")
@SpringBootTest(properties = {
        "eaf.security.mode=enterprise",
        "eaf.security.enterprise.issuer=https://recovery-idp.example.test",
        "eaf.security.enterprise.jwks-uri=https://recovery-idp.example.test/jwks",
        "eaf.security.enterprise.audience=eaf:p7-recovery-test",
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false"
})
class P7RestoreReconciliationTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired WorkspaceOperationalControl operationalControl;
    @Autowired WorkspaceAuthorization workspaces;
    @Autowired AuditPort audit;
    @Autowired Clock clock;
    @Autowired Environment environment;

    @BeforeEach
    void prepareWorkspace() {
        jdbc.update("update workspace.operational_gate set enabled = true where tenant_id = ? and workspace_id = ?",
                Ids.TENANT_A, Ids.WORKSPACE_A);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'workspace:operations:stop', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, 'workspace:operations:resume', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
    }

    @Test
    void recoveryProfileClosesBothGatesAndOnlyOwnerStopCanPersistTheBarrier() {
        assertThat(environment.getActiveProfiles()).contains("recovery");
        assertThat(environment.getProperty("eaf.recovery.mode", Boolean.class)).isTrue();
        assertThat(environment.getProperty("eaf.security.mode")).isEqualTo("enterprise");
        assertThat(environment.getProperty("eaf.secrets.backend")).isEqualTo("unconfigured");
        assertThat(environment.getProperty("eaf.outbound.enterprise.egress-policy-confirmed", Boolean.class)).isFalse();
        assertThat(environment.getProperty("eaf.task.dispatcher-enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("eaf.workflow.dispatcher-enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("eaf.execution.outbox-publisher-enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("eaf.knowledge.outbox-publisher-enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("eaf.memory.outbox-publisher-enabled", Boolean.class)).isFalse();

        // 恢复模式运行时关闸，但不伪造修改者或覆盖旧备份中的状态/版本。
        assertThat(jdbc.queryForList("select enabled from workspace.operational_gate where tenant_id = ? "
                        + "and workspace_id = ? order by gate_name", Boolean.class, Ids.TENANT_A, Ids.WORKSPACE_A))
                .containsExactly(true, true);
        assertThat(operationalControl.taskAdmissionOpen(Ids.TENANT_A, Ids.WORKSPACE_A)).isFalse();
        assertThat(operationalControl.businessOutboundOpen(Ids.TENANT_A, Ids.WORKSPACE_A)).isFalse();

        var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());
        operationalControl.stopTaskAdmission(bob, Ids.WORKSPACE_A, "restore-stop-task-" + java.util.UUID.randomUUID(),
                false, "恢复对账期间暂停新任务");
        operationalControl.stopBusinessOutbound(bob, Ids.WORKSPACE_A, "restore-stop-outbound-" + java.util.UUID.randomUUID(),
                false, "恢复对账期间暂停业务出站");

        assertThat(jdbc.queryForList("select enabled from workspace.operational_gate where tenant_id = ? "
                        + "and workspace_id = ? order by gate_name", Boolean.class, Ids.TENANT_A, Ids.WORKSPACE_A))
                .containsExactly(false, false);
        assertThatThrownBy(() -> operationalControl.stopTaskAdmission(bob, Ids.WORKSPACE_A,
                "restore-reopen-" + java.util.UUID.randomUUID(), true, "恢复对账尚未结束"))
                .isInstanceOfSatisfying(EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("RECOVERY_RECONCILIATION_REQUIRED"));
    }

    @Test
    void recoveryStartupRejectsEgressOrSchedulerOverrides() {
        var unsafe = new RecoveryModeInvariant(true, "enterprise", "unconfigured", true,
                false, false, false, false, false, false);
        assertThatThrownBy(unsafe::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("关闭出站");
        var unsafeSecrets = new RecoveryModeInvariant(true, "enterprise", "environment", false,
                false, false, false, false, false, false);
        assertThatThrownBy(unsafeSecrets::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("秘密后端");
    }

    @Test
    void isolatedPostgresDumpRestorePreservesEvidenceAndRecoveryBarrier() throws Exception {
        jdbc.execute("create schema if not exists p7_restore_fixture");
        jdbc.execute("create table if not exists p7_restore_fixture.marker (marker_id text primary key, marker_value text not null)");
        jdbc.update("insert into p7_restore_fixture.marker(marker_id, marker_value) values ('drill-001', '外部核对前保持隔离') "
                + "on conflict (marker_id) do update set marker_value = excluded.marker_value");
        var dump = postgres.execInContainer("pg_dump", "--format=plain", "--no-owner", "--no-acl",
                "--username", postgres.getUsername(), "--dbname", postgres.getDatabaseName());
        assertThat(dump.getExitCode()).isZero();

        var dumpFile = Files.createTempFile("eaf-p7-restore-", ".sql");
        var restoredPostgres = new PostgreSQLContainer<>(
                DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
        try {
            Files.writeString(dumpFile, dump.getStdout());
            restoredPostgres.start();
            restoredPostgres.copyFileToContainer(MountableFile.forHostPath(dumpFile.toAbsolutePath()),
                    "/tmp/eaf-p7-restore.sql");
            var restore = restoredPostgres.execInContainer("psql", "--set", "ON_ERROR_STOP=1", "--username",
                    restoredPostgres.getUsername(), "--dbname", restoredPostgres.getDatabaseName(),
                    "--file", "/tmp/eaf-p7-restore.sql");
            assertThat(restore.getExitCode()).isZero();

            var restoredJdbc = new JdbcTemplate(new DriverManagerDataSource(restoredPostgres.getJdbcUrl(),
                    restoredPostgres.getUsername(), restoredPostgres.getPassword()));
            assertThat(restoredJdbc.queryForObject("select marker_value from p7_restore_fixture.marker where marker_id = 'drill-001'",
                    String.class)).isEqualTo("外部核对前保持隔离");
            // 备份恢复保留当前数据库结构版本。
            assertThat(restoredJdbc.queryForObject("select version from eaf_meta.flyway_schema_history "
                    + "where success order by installed_rank desc limit 1", String.class)).isEqualTo("176");
            assertThat(restoredJdbc.queryForList("select enabled from workspace.operational_gate where tenant_id = ? "
                            + "and workspace_id = ? order by gate_name", Boolean.class, Ids.TENANT_A, Ids.WORKSPACE_A))
                    .containsExactly(true, true);

            // 恢复后先用同一 Owner 实现开启运行时屏障；该屏障不写入无法证明当前性的授权表。
            var restoredOperations = new JdbcWorkspaceOperationalControl(restoredJdbc, workspaces, audit, clock, true);
            assertThat(restoredOperations.taskAdmissionOpen(Ids.TENANT_A, Ids.WORKSPACE_A)).isFalse();
            assertThat(restoredOperations.businessOutboundOpen(Ids.TENANT_A, Ids.WORKSPACE_A)).isFalse();
            assertThat(restoredJdbc.queryForObject("select marker_value from p7_restore_fixture.marker where marker_id = 'drill-001'",
                    String.class)).isEqualTo("外部核对前保持隔离");
        } finally {
            restoredPostgres.stop();
            Files.deleteIfExists(dumpFile);
        }
    }
}
