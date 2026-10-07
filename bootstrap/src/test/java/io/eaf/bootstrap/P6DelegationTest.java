package io.eaf.bootstrap;

import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.IdentityService;
import io.eaf.policy.api.PolicyRequest;
import io.eaf.policy.api.PolicyService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.time.Clock;
import java.time.Duration;
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
@SpringBootTest
class P6DelegationTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.task.dispatcher-enabled", () -> "false");
    }

    @Autowired IdentityService identities;
    @Autowired PolicyService policy;
    @Autowired TaskService tasks;
    @Autowired JdbcTemplate jdbc;
    @Autowired Clock clock;

    // 该反例集锁定最小单跳权限边界，避免把身份解析、资源读取和 Task 创建混为一种授权。
    @Test
    void delegatesStayBoundToOwnerAgentWorkspaceScopeAndTaskSnapshot() {
        var owner = identities.resolveToken("alice").orElseThrow();
        var valid = create(owner, Set.of("task:create", "task:read", "crm:customer:read"), Set.of("customer-001"));
        var delegate = identities.resolveDelegatedToken("risk-agent", valid.id(), IdentityService.REST_AUDIENCE).orElseThrow();
        assertThat(delegate.type()).isEqualTo(ActorType.AGENT);
        assertThat(delegate.actorId()).isEqualTo(Ids.AGENT_RISK);
        assertThat(delegate.principalId()).isEqualTo(Ids.ALICE);
        assertThat(delegate.actions()).containsExactlyInAnyOrder("task:create", "task:read", "crm:customer:read");
        assertThat(identities.resolveDelegatedToken("alice", valid.id(), IdentityService.REST_AUDIENCE)).isEmpty();
        assertThat(identities.resolveDelegation(Ids.TENANT_A, Ids.ALICE, Ids.AGENT_RISK, valid.id(), Ids.WORKSPACE_A, "eaf:other")).isEmpty();
        assertThat(identities.resolveDelegation(Ids.TENANT_B, Ids.ALICE, Ids.AGENT_RISK, valid.id(), Ids.WORKSPACE_A, IdentityService.REST_AUDIENCE)).isEmpty();

        // 身份图由服务端限制为 HUMAN→AGENT 一跳，不能自委托，也不能让 Agent 转授或回环。
        assertThatThrownBy(() -> identities.createDelegation(new CreateDelegationCommand(owner, Ids.WORKSPACE_A,
                owner.actorId(), Set.of("task:create"), Set.of(), Instant.now(clock).plusSeconds(60))))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> identities.createDelegation(new CreateDelegationCommand(delegate, Ids.WORKSPACE_A,
                owner.actorId(), Set.of("task:create"), Set.of(), Instant.now(clock).plusSeconds(60))))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> identities.createDelegation(new CreateDelegationCommand(delegate, Ids.WORKSPACE_A,
                Ids.AGENT_RISK_B, Set.of("task:create"), Set.of(), Instant.now(clock).plusSeconds(60))))
                .isInstanceOf(EafException.class);

        assertThatThrownBy(() -> create(owner, Set.of("task:create", "task:cancel"), Set.of()))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> create(owner, Set.of("task:create", "crm:customer:read"), Set.of("customer-002")))
                .isInstanceOf(EafException.class);
        assertThatThrownBy(() -> identities.createDelegation(new CreateDelegationCommand(delegate, Ids.WORKSPACE_A,
                Ids.AGENT_RISK, Set.of("task:create"), Set.of(), Instant.now(clock).plusSeconds(60))))
                .isInstanceOf(EafException.class);

        var read = policy.evaluate(new PolicyRequest(delegate, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "crm.customer.query", "1.0.0", "READ", "customer-001", "USER"));
        var outOfScopeRead = policy.evaluate(new PolicyRequest(delegate, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "crm.customer.query", "1.0.0", "READ", "customer-002", "USER"));
        var write = policy.evaluate(new PolicyRequest(delegate, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "crm.followup.create", "1.0.0", "WRITE", "customer-001", "USER"));
        assertThat(read.allowed()).isTrue();
        assertThat(outOfScopeRead.allowed()).isFalse();
        assertThat(write.allowed()).isFalse();

        var task = createTask(delegate, "task-delegation-revoke");
        var stored = jdbc.queryForMap("select actor_id, principal_id, delegate_id, delegation_id, authorization_hash from task.task where id = ?", task.id());
        assertThat(stored.get("actor_id")).isEqualTo(Ids.AGENT_RISK);
        assertThat(stored.get("principal_id")).isEqualTo(Ids.ALICE);
        assertThat(stored.get("delegate_id")).isEqualTo(Ids.AGENT_RISK);
        assertThat(stored.get("delegation_id")).isEqualTo(valid.id());
        assertThat(stored.get("authorization_hash")).isEqualTo(delegate.authorizationHash());

        var other = create(owner, Set.of("task:create", "task:read"), Set.of());
        var otherActor = identities.resolveDelegatedToken("risk-agent", other.id(), IdentityService.REST_AUDIENCE).orElseThrow();
        assertThatThrownBy(() -> tasks.get(otherActor, Ids.WORKSPACE_A, task.id())).isInstanceOf(EafException.class);
        identities.revokeDelegation(owner, Ids.WORKSPACE_A, valid.id());
        assertThat(identities.resolveDelegatedToken("risk-agent", valid.id(), IdentityService.REST_AUDIENCE)).isEmpty();
        assertThat(tasks.claimOne()).isEmpty();
        assertThat(tasks.get(owner, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);

        var expiring = create(owner, Set.of("task:create", "task:read"), Set.of());
        var expiringActor = identities.resolveDelegatedToken("risk-agent", expiring.id(), IdentityService.REST_AUDIENCE).orElseThrow();
        var expiringTask = createTask(expiringActor, "task-delegation-expire");
        jdbc.update("update identity.delegation set expires_at = ? where id = ?", java.sql.Timestamp.from(Instant.now(clock).minus(Duration.ofSeconds(1))), expiring.id());
        assertThat(tasks.claimOne()).isEmpty();
        assertThat(tasks.get(owner, Ids.WORKSPACE_A, expiringTask.id()).status()).isEqualTo(TaskStatus.FAILED);
    }

    private io.eaf.identity.api.DelegationSnapshot create(ActorContext owner, Set<String> actions, Set<String> customers) {
        return identities.createDelegation(new CreateDelegationCommand(owner, Ids.WORKSPACE_A, Ids.AGENT_RISK,
                actions, customers, Instant.now(clock).plus(Duration.ofMinutes(5))));
    }

    private io.eaf.task.api.TaskSnapshot createTask(ActorContext actor, String key) {
        return tasks.create(new CreateTaskCommand(actor, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0", "Analyze customer risk",
                null, null, key, UUID.randomUUID().toString(), "USER"));
    }
}
