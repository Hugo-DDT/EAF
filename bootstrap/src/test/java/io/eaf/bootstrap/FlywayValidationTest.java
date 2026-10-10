package io.eaf.bootstrap;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.exception.FlywayValidateException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlywayValidationTest {

    // Flyway 校验也必须使用带 vector 扩展的数据库，避免迁移环境与运行环境分叉。
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Test
    void migrationIsIdempotentAndChecksumChangesFailValidation() {
        try (var postgres = new PostgreSQLContainer<>(
                DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"))) {
            postgres.start();
            var original = flyway(postgres, "classpath:db/checksum/initial");
            original.migrate();
            original.migrate();

            assertThatThrownBy(() -> flyway(postgres, "classpath:db/checksum/changed").validate())
                    .isInstanceOf(FlywayValidateException.class);
        }
    }

    // 从 V100 升级至当前版本；核实向量兼容、用量演进及凭据迁移仅保存秘密引用。
    @Test
    void v100DatabaseUpgradesThroughCurrentMigrationsWithoutChangingTheBaseline() {
        try (var postgres = new PostgreSQLContainer<>(
                DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"))) {
            postgres.start();
            var locations = new String[] {"classpath:db/test-migration", "classpath:db/migration"};
            var atV100 = Flyway.configure()
                    .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations(locations).createSchemas(true).defaultSchema("eaf_meta").schemas("eaf_meta")
                    .target(MigrationVersion.fromVersion("100")).load();
            atV100.migrate();
            assertThat(atV100.info().current().getVersion().getVersion()).isEqualTo("100");

            var jdbc = new JdbcTemplate(new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
            assertThat(jdbc.queryForObject("select count(*) from capability.version where capability_id = '54000000-0000-4000-8000-000000000001' and asset_version = '1.1.0'", Integer.class)).isZero();
            var throughV103 = Flyway.configure()
                    .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations(locations).createSchemas(true).defaultSchema("eaf_meta").schemas("eaf_meta")
                    .target(MigrationVersion.fromVersion("103")).load();
            throughV103.migrate();
            assertThat(throughV103.info().current().getVersion().getVersion()).isEqualTo("103");

            var legacyDocument = java.util.UUID.fromString("a1000000-0000-4000-8000-000000000001");
            var legacyChunk = java.util.UUID.fromString("a1000000-0000-4000-8000-000000000002");
            var legacyBuild = java.util.UUID.fromString("a1000000-0000-4000-8000-000000000003");
            var legacyEmbedding = java.util.UUID.fromString("a1000000-0000-4000-8000-000000000004");
            var legacySignature = "a".repeat(64);
            var legacyTenant = java.util.UUID.fromString("70000000-0000-4000-8000-000000000001");
            var legacyWorkspace = java.util.UUID.fromString("10000000-0000-4000-8000-000000000001");
            var legacyOwner = java.util.UUID.fromString("80000000-0000-4000-8000-000000000001");
            // 测试夹具模拟旧版发布过的向量和活动指针，不预先引入列或约束。
            jdbc.update("insert into knowledge.document(id, tenant_id, workspace_id, owner_id, title, source_ref, metadata, status, idempotency_key, request_hash, row_version, created_at, updated_at) values (?, ?, ?, ?, 'legacy vector', 'manual://v103', '{}'::jsonb, 'PUBLISHED', 'legacy-v103', ?, 2, now(), now())",
                    legacyDocument, legacyTenant, legacyWorkspace, legacyOwner, legacySignature);
            jdbc.update("insert into knowledge.document_version(id, tenant_id, workspace_id, document_id, asset_version, content, content_hash, status, created_at, idempotency_key, request_hash) values (?, ?, ?, ?, 1, 'legacy content', ?, 'PUBLISHED', now(), 'legacy-v103', ?)",
                    java.util.UUID.fromString("a1000000-0000-4000-8000-000000000005"), legacyTenant, legacyWorkspace,
                    legacyDocument, legacySignature, legacySignature);
            jdbc.update("insert into knowledge.chunk(id, tenant_id, workspace_id, document_id, asset_version, source_ref, chunking_version, chunk_order, start_offset, end_offset, offset_unit, content, content_hash, created_at) values (?, ?, ?, ?, 1, 'manual://v103', 'p3-plain-1', 1, 0, 14, 'UNICODE_CODE_POINT', 'legacy content', ?, now())",
                    legacyChunk, legacyTenant, legacyWorkspace, legacyDocument, legacySignature);
            jdbc.update("insert into knowledge.index_build(id, tenant_id, workspace_id, document_id, asset_version, chunking_version, provider, model, model_revision, dimension, distance_metric, configuration_signature, idempotency_key, status, total_chunks, completed_chunks, next_chunk_order, input_tokens, embedding_calls, created_at, completed_at, updated_at) values (?, ?, ?, ?, 1, 'p3-plain-1', 'deterministic', 'p3-test-embedding-8', 'SHA256-NORMALIZED-V1', 8, 'COSINE', ?, 'legacy-v103', 'READY', 1, 1, 2, 4, 1, now(), now(), now())",
                    legacyBuild, legacyTenant, legacyWorkspace, legacyDocument, legacySignature);
            jdbc.update("insert into knowledge.embedding(id, tenant_id, workspace_id, document_id, asset_version, chunk_id, build_id, provider, model, model_revision, dimension, chunking_version, distance_metric, configuration_signature, embedding, created_at) values (?, ?, ?, ?, 1, ?, ?, 'deterministic', 'p3-test-embedding-8', 'SHA256-NORMALIZED-V1', 8, 'p3-plain-1', 'COSINE', ?, '[1,0,0,0,0,0,0,0]'::public.vector, now())",
                    legacyEmbedding, legacyTenant, legacyWorkspace, legacyDocument, legacyChunk, legacyBuild, legacySignature);
            jdbc.update("insert into knowledge.document_publication(tenant_id, workspace_id, document_id, asset_version, build_id, status, row_version, updated_at) values (?, ?, ?, 1, ?, 'ACTIVE', 1, now())",
                    legacyTenant, legacyWorkspace, legacyDocument, legacyBuild);

            var legacyTask = java.util.UUID.fromString("a0000000-0000-4000-8000-000000000001");
            var legacyRun = java.util.UUID.fromString("a0000000-0000-4000-8000-000000000002");
            jdbc.update("insert into usage.model_usage(id, tenant_id, workspace_id, task_id, run_id, call_no, source, provider, model, usage_status, reserved_tokens, status, started_at, ended_at) "
                            + "values ('a0000000-0000-4000-8000-000000000003', '70000000-0000-4000-8000-000000000001', "
                            + "'10000000-0000-4000-8000-000000000001', ?, ?, 2, 'USER', 'legacy-provider', 'legacy-model', 'UNKNOWN', 100, 'FAILED', now(), now())",
                    legacyTask, legacyRun);

            var latest = Flyway.configure()
                    .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations(locations).createSchemas(true).defaultSchema("eaf_meta").schemas("eaf_meta").load();
            latest.migrate();

            //  从 V100 完整升级到当前迁移，同时保留下面逐项核对的历史数据。
            assertThat(latest.info().current().getVersion().getVersion()).isEqualTo("266");
            assertThat(jdbc.queryForObject("select to_regclass('task.conversation') is not null and to_regclass('task.conversation_turn') is not null and to_regclass('task.conversation_brief_revision') is not null and to_regclass('task.conversation_brief_save') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select to_regclass('task.customer_followup') is not null and to_regclass('task.customer_followup_result') is not null and to_regclass('task.customer_followup_command') is not null and to_regclass('task.customer_followup_sync') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select count(*) from agent.version where response_profile in ('CONVERSATIONAL_KNOWLEDGE_QA_V1','CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1') and status = 'PUBLISHED'", Integer.class)).isEqualTo(2);
            assertThat(jdbc.queryForObject("select status from workflow.version where workflow_id = '58000000-0000-4000-8000-000000000009' and asset_version = '1.2.0'", String.class)).isEqualTo("PUBLISHED");
            assertThat(jdbc.queryForObject("select count(*) from workflow.capability_dependency where workflow_id = '58000000-0000-4000-8000-000000000009' and workflow_version = '1.2.0' and capability_id = '54000000-0000-4000-8000-000000000001' and capability_version = '1.5.0'", Integer.class)).isEqualTo(1);
            // V108—V135 增加隔离评测、受保护身份、版本化凭据及 CRM 只读/审批写入契约夹具。
            assertThat(jdbc.queryForObject("select to_regclass('credential.binding') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select to_regclass('credential.secret_version') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select count(*) from credential.binding where scope_type = 'SYSTEM'", Integer.class)).isEqualTo(3);
            assertThat(jdbc.queryForObject("select count(*) from credential.binding where credential_ref = 'model-typesafe' and allowed_uses @> array['model.decision'] and permissions @> array['provider:typesafe:invoke']", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from credential.secret_version where secret_ref !~ '^(env|vault|aws-sm|azure-kv)://'", Integer.class)).isZero();
            assertThat(jdbc.queryForObject("select to_regclass('evaluation.candidate_dev_case') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select to_regclass('evaluation.candidate_eval_manual_review') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select to_regclass('evaluation.collaboration_analysis_manifest') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select count(*) from pg_attribute where attrelid = 'evaluation.eval_run'::regclass and attname in ('configuration_hash','manifest_snapshot','lease_owner','stop_requested','estimated_cost','latency_p95_ms') and not attisdropped", Integer.class)).isEqualTo(6);
            assertThat(jdbc.queryForObject("select count(*) from pg_attribute where attrelid = 'task.task'::regclass and attname = 'quality_run_id' and not attisdropped", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from pg_attribute where attrelid = 'workflow.instance'::regclass and attname = 'quality_run_id' and not attisdropped", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from workflow.version where workflow_id in ('58000000-0000-4000-8000-000000000002', '58000000-0000-4000-8000-000000000003')", Integer.class)).isEqualTo(2);
            assertThat(jdbc.queryForObject("select count(*) from evaluation.collaboration_analysis_manifest where purpose = 'COLLABORATION_HELD_OUT' and source = 'EVALUATION' and status = 'ACTIVE'", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from pg_attribute where attrelid = 'task.task'::regclass and attname in ('workflow_instance_id','workflow_id','workflow_version','workflow_step_id') and not attisdropped", Integer.class)).isEqualTo(4);
            assertThat(jdbc.queryForObject("select status from connector.instance where provider = 'A2A_EVALUATION_REVIEW_PEER' and workspace_id = '10000000-0000-4000-8000-000000000001'", String.class)).isEqualTo("DISABLED");
            assertThat(jdbc.queryForObject("select status from connector.instance where provider = 'P7_CRM_WRITE_CONTRACT_FIXTURE' and workspace_id = '10000000-0000-4000-8000-000000000001'", String.class)).isEqualTo("DISABLED");
            assertThat(jdbc.queryForObject("select count(*) from credential.binding where credential_ref = 'p7-crm-write' and allowed_uses @> array['customer.read','followup.create','followup.verify'] and permissions @> array['crm.customer.read','crm.followup.create','crm.followup.read']", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from credential.binding where id = 'a7000000-0000-4000-8000-000000000015' and credential_ref = 'p12-crm-outcome' and scope_type = 'WORKSPACE' and allowed_uses @> array['followup.result.create','followup.result.verify'] and permissions @> array['crm.followup.result','crm.followup.read']", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from credential.secret_version where binding_id = 'a7000000-0000-4000-8000-000000000015' and version = 1 and secret_ref = 'env://eaf.credentials.p12-crm-outcome.token' and status = 'ACTIVE'", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from tool.version where name = 'crm.followup.create' and asset_version = '1.1.0' and effect = 'WRITE' and binding_ref = 'p7-crm-write-contract.followup-create'", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from agent.tool_binding where agent_version = '2.2.0' and tool_name in ('crm.customer.query','crm.followup.create')", Integer.class)).isEqualTo(2);
            assertThat(jdbc.queryForObject("select credential_ref || ':' || audience from connector.instance where provider = 'A2A_EVALUATION_REVIEW_PEER' and workspace_id = '10000000-0000-4000-8000-000000000001'", String.class)).isEqualTo("a2a-evaluation-review:eaf:evaluation-peer");
            assertThat(jdbc.queryForObject("select count(*) from tool.version where name = 'agent.risk.review.evaluation' and asset_version = '1.0.0' and effect = 'READ' and permission_action = 'agent:risk-review-evaluation' and binding_ref = 'A2A_EVALUATION_REVIEW_PEER:risk-review-evaluation@1.0.0'", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select status from capability.version where capability_id = '54000000-0000-4000-8000-000000000001' and asset_version = '1.2.0'", String.class)).isEqualTo("DRAFT");
            assertThat(jdbc.queryForObject("select resource_scope from agent.remote_registration where agent_key = 'risk-review-evaluation' and workspace_id = '10000000-0000-4000-8000-000000000001'", String.class)).isEqualTo("SYNTHETIC_EVALUATION_ONLY");
            assertThat(jdbc.queryForObject("select steps_json @> '[{\"id\":\"analyze\",\"capabilityVersion\":\"1.2.0\"},{\"id\":\"peer-review\",\"toolName\":\"agent.risk.review.evaluation\",\"toolVersion\":\"1.0.0\",\"capabilityVersion\":\"1.2.0\"}]'::jsonb from workflow.version where workflow_id = '58000000-0000-4000-8000-000000000003' and asset_version = '1.0.0'", Boolean.class)).isTrue();
            //  迁移放宽向量 typmod，但旧 8 维数据、READY 构建和活动发布指针必须原样保留。
            assertThat(jdbc.queryForObject("select format_type(a.atttypid, a.atttypmod) from pg_attribute a where a.attrelid = 'knowledge.embedding'::regclass and a.attname = 'embedding'", String.class)).isEqualTo("vector");
            assertThat(jdbc.queryForObject("select vector_dims(embedding) from knowledge.embedding where id = ?", Integer.class, legacyEmbedding)).isEqualTo(8);
            assertThat(jdbc.queryForObject("select status from knowledge.index_build where id = ?", String.class, legacyBuild)).isEqualTo("READY");
            assertThat(jdbc.queryForObject("select build_id from knowledge.document_publication where document_id = ? and status = 'ACTIVE'", java.util.UUID.class, legacyDocument)).isEqualTo(legacyBuild);
            assertThat(jdbc.queryForObject("select max_input_tokens from knowledge.index_build where id = ?", Integer.class, legacyBuild)).isEqualTo(8192);
            // V106 仍只新增持久金额预算表，旧用量迁移不虚构费率或支出。
            assertThat(jdbc.queryForObject("select to_regclass('usage.spend_scope') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select to_regclass('usage.spend_reservation') is not null", Boolean.class)).isTrue();
            assertThat(jdbc.queryForObject("select count(*) from usage.spend_scope", Integer.class)).isZero();
            assertThat(jdbc.queryForObject("select status from capability.version where capability_id = '54000000-0000-4000-8000-000000000001' and asset_version = '1.1.0'", String.class)).isEqualTo("DRAFT");
            assertThat(jdbc.queryForObject("select status from workflow.version where workflow_id = '58000000-0000-4000-8000-000000000001' and asset_version = '1.1.0'", String.class)).isEqualTo("DRAFT");
            assertThat(jdbc.queryForObject("select count(*) from workflow.version where workflow_id = '58000000-0000-4000-8000-000000000001' and asset_version in ('1.0.0', '1.1.0')", Integer.class)).isEqualTo(2);
            assertThat(jdbc.queryForObject("select status from workspace.\"grant\" where actor_id = '20000000-0000-4000-8000-000000000001' and workspace_id = '10000000-0000-4000-8000-000000000001' and action = 'agent:risk-review'", String.class)).isEqualTo("ACTIVE");
            assertThat(jdbc.queryForObject("select status from workspace.\"grant\" where actor_id = '20000000-0000-4000-8000-000000000001' and workspace_id = '10000000-0000-4000-8000-000000000001' and action = 'tool:read'", String.class)).isEqualTo("ACTIVE");
            assertThat(jdbc.queryForObject("select call_key from usage.model_usage where id = 'a0000000-0000-4000-8000-000000000003'", String.class))
                    .isEqualTo("task:" + legacyTask + ":run:" + legacyRun + ":call:2");
            assertThat(jdbc.queryForObject("select scope_type from usage.model_usage where id = 'a0000000-0000-4000-8000-000000000003'", String.class)).isEqualTo("TASK");
            assertThat(jdbc.queryForObject("select cost_status from usage.model_usage where id = 'a0000000-0000-4000-8000-000000000003'", String.class)).isEqualTo("UNKNOWN_PRICE");
            assertThat(jdbc.queryForObject("select estimated_cost from usage.model_usage where id = 'a0000000-0000-4000-8000-000000000003'", java.math.BigDecimal.class)).isNull();
            assertThat(jdbc.queryForObject("select count(*) from usage.price_schedule", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("select count(*) from usage.price_schedule where provider = 'typesafe' and model = 'jev-1.13.0' and call_type = 'DECISION' and input_price_per_million = 0.04200000 and output_price_per_million = 0", Integer.class)).isEqualTo(1);
        }
    }

    private Flyway flyway(PostgreSQLContainer<?> postgres, String location) {
        return Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .locations(location)
                .load();
    }
}
// 本文件负责实现 EAF 的 FlywayValidationTest.java 相关代码。
