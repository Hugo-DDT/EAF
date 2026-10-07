package io.eaf.bootstrap;

import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.skill.api.CreateSkillVersionCommand;
import io.eaf.skill.api.SkillService;
import io.eaf.skill.api.SkillToolReference;
import io.eaf.tool.api.ToolCatalog;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 使用真实隔离 PostgreSQL 验证 Skill 迁移、版本不变式及发布依赖的负例。
class SkillLifecycleTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext BOB = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final UUID SEEDED_SKILL = UUID.fromString("53000000-0000-4000-8000-000000000001");

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

    @Autowired SkillService skills;
    @Autowired PromptCatalog prompts;
    @Autowired ToolCatalog tools;
    @Autowired JdbcTemplate jdbc;

    @Test
    void publishedSkillResolvesExactDependenciesAndRejectsSchemaDrift() {
        var skill = skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0");
        assertThat(skill.name()).isEqualTo("customer-risk-analysis");
        assertThat(skill.promptVersion()).isEqualTo("2.0.0");
        assertThat(skill.toolDependencies()).extracting("name").containsExactly("crm.customer.query");
        assertThat(skill.contentHash()).hasSize(64);

        var tool = tools.requirePublished(Ids.TENANT_A, Ids.WORKSPACE_A, "crm.customer.query", "1.0.0");
        jdbc.update("update tool.version set input_schema = ?::jsonb where tenant_id = ? and workspace_id = ? and name = ? and asset_version = ?",
                "{\"type\":\"object\",\"required\":[\"customerId\"],\"properties\":{\"customerId\":{\"type\":\"integer\"}}}",
                Ids.TENANT_A, Ids.WORKSPACE_A, tool.name(), tool.version());
        try {
            assertThatThrownBy(() -> skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0"))
                    .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("SKILL_TOOL_SCHEMA_CONFLICT"));
        } finally {
            jdbc.update("update tool.version set input_schema = ?::jsonb where tenant_id = ? and workspace_id = ? and name = ? and asset_version = ?",
                    tool.inputSchema(), Ids.TENANT_A, Ids.WORKSPACE_A, tool.name(), tool.version());
        }

        jdbc.update("update prompt.version set status = 'REVOKED' where id = ? and workspace_id = ? and asset_version = ?",
                Ids.PROMPT_RISK, Ids.WORKSPACE_A, "2.0.0");
        try {
            assertThatThrownBy(() -> skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0"))
                    .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
        } finally {
            jdbc.update("update prompt.version set status = 'PUBLISHED' where id = ? and workspace_id = ? and asset_version = ?",
                    Ids.PROMPT_RISK, Ids.WORKSPACE_A, "2.0.0");
        }
    }

    @Test
    void publishingNewVersionDoesNotChangeOrReviveOldVersions() {
        var original = skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0");
        //  占用 Skill 1.1.0—1.2.0， 使用 1.3.0；本测试使用 1.4.0 验证发布后旧版本不变。
        var next = skills.addVersion(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL,
                new CreateSkillVersionCommand("1.4.0", original.inputSchema(),
                        "{\"type\":\"object\",\"required\":[\"riskLevel\",\"summary\",\"reasons\",\"uncertainties\"],\"properties\":{\"riskLevel\":{\"type\":\"string\"},\"summary\":{\"type\":\"string\"},\"reasons\":{\"type\":\"array\"},\"uncertainties\":{\"type\":\"array\"},\"evidenceClass\":{\"type\":\"string\"}}}",
                        original.promptId(), original.promptVersion(), List.of(new SkillToolReference("crm.customer.query", "1.0.0")), "p2-v1"));
        assertThat(next.status()).isEqualTo("DRAFT");

        var published = skills.publish(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.4.0", next.rowVersion());
        assertThat(published.status()).isEqualTo("PUBLISHED");
        assertThat(skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0").contentHash())
                .isEqualTo(original.contentHash());
        assertThat(skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0").outputSchema())
                .doesNotContain("evidenceClass");

        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'skill:publish', 'ACTIVE')",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        try {
            assertThatThrownBy(() -> skills.revoke(BOB, Ids.WORKSPACE_A, SEEDED_SKILL, "1.4.0", published.rowVersion()))
                    .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(403));
        } finally {
            jdbc.update("delete from workspace.\"grant\" where workspace_id = ? and actor_id = ? and action = 'skill:publish'",
                    Ids.WORKSPACE_A, Ids.BOB);
        }

        var revoked = skills.revoke(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.4.0", published.rowVersion());
        assertThat(revoked.status()).isEqualTo("REVOKED");
        assertThatThrownBy(() -> skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.4.0"))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
        assertThat(skills.requirePublished(ALICE, Ids.WORKSPACE_A, SEEDED_SKILL, "1.0.0").status()).isEqualTo("PUBLISHED");
    }

    @Test
    void invalidSchemasAndInvisibleDependenciesAreRejectedBeforeSaving() {
        var invalid = new CreateSkillVersionCommand("1.0.0",
                "{\"type\":\"object\",\"required\":[\"missing\"],\"properties\":{\"input\":{\"type\":\"string\"}}}",
                "{\"type\":\"object\",\"properties\":{\"result\":{\"type\":\"string\"}}}",
                Ids.PROMPT_RISK, "2.0.0", List.of(new SkillToolReference("crm.customer.query", "1.0.0")), "p2-v1");
        assertThatThrownBy(() -> skills.create(new io.eaf.skill.api.CreateSkillCommand(ALICE, Ids.WORKSPACE_A,
                "invalid-schema-" + UUID.randomUUID(), "invalid schema", invalid)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("INVALID_REQUEST"));

        var hidden = new CreateSkillVersionCommand("1.0.0", "{\"type\":\"object\",\"properties\":{\"input\":{\"type\":\"string\"}}}",
                "{\"type\":\"object\",\"properties\":{\"result\":{\"type\":\"string\"}}}",
                UUID.fromString("21000000-0000-4000-8000-000000000002"), "1.0.0",
                List.of(new SkillToolReference("crm.customer.query", "1.0.0")), "p2-v1");
        assertThatThrownBy(() -> skills.create(new io.eaf.skill.api.CreateSkillCommand(ALICE, Ids.WORKSPACE_A,
                "hidden-dependency-" + UUID.randomUUID(), "hidden prompt", hidden)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));

        var missingTool = new CreateSkillVersionCommand("1.0.0", "{\"type\":\"object\",\"properties\":{\"input\":{\"type\":\"string\"}}}",
                "{\"type\":\"object\",\"properties\":{\"result\":{\"type\":\"string\"}}}",
                Ids.PROMPT_RISK, "2.0.0", List.of(new SkillToolReference("missing.tool", "1.0.0")), "p2-v1");
        assertThatThrownBy(() -> skills.create(new io.eaf.skill.api.CreateSkillCommand(ALICE, Ids.WORKSPACE_A,
                "missing-dependency-" + UUID.randomUUID(), "missing tool", missingTool)))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));

        assertThatThrownBy(() -> jdbc.update("update skill.version set input_schema = '{}'::jsonb where skill_id = ? and asset_version = '1.0.0'", SEEDED_SKILL))
                .isInstanceOf(DataAccessException.class);
        assertThatThrownBy(() -> jdbc.update("delete from skill.tool_dependency where skill_id = ? and skill_version = '1.0.0'", SEEDED_SKILL))
                .isInstanceOf(DataAccessException.class);
    }
}
