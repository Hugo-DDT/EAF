package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = "eaf.task.dispatcher-enabled=false")
@AutoConfigureMockMvc
class P3KnowledgeImportTest {
    // 知识迁移已包含 vector 扩展，导入与分块测试必须在同一真实数据库能力上运行。
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE_A = "10000000-0000-4000-8000-000000000001";
    private static final String WORKSPACE_A2 = "10000000-0000-4000-8000-000000000002";
    private static final String WORKSPACE_B = "10000000-0000-4000-8000-000000000003";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired DataSource dataSource;

    @Test
    void importsReadsAndScopesDraftDocument() throws Exception {
        var body = body("客户服务规范", "policy://retention/v1", "客户续约前必须核验最近联系记录。", MapEntry.of("category", "policy"));
        var created = mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-import-basic")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("DRAFT"))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.ownerId").value("80000000-0000-4000-8000-000000000001"))
                .andReturn();
        var documentId = mapper.readTree(created.getResponse().getContentAsString()).path("id").asText();

        mvc.perform(get(path(WORKSPACE_A) + "/" + documentId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").value("客户续约前必须核验最近联系记录。"))
                .andExpect(jsonPath("$.metadata.category").value("policy"));
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-import-basic")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(documentId));
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-import-basic")
                        .contentType(MediaType.APPLICATION_JSON).content(body.replace("续约", "投诉")))
                .andExpect(status().isConflict());

        mvc.perform(get(path(WORKSPACE_A) + "/" + documentId)
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isNotFound());
        mvc.perform(get(path(WORKSPACE_B) + "/" + documentId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isNotFound());
        mvc.perform(post(path(WORKSPACE_A2))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-import-no-write")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isForbidden());

        var jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document where id = ?", Integer.class, UUID.fromString(documentId))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_version where document_id = ?", Integer.class, UUID.fromString(documentId))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_permission where document_id = ? and actor_id = '80000000-0000-4000-8000-000000000001'", Integer.class, UUID.fromString(documentId))).isEqualTo(3);
    }

    @Test
    void rejectsInvalidMetadataUnknownOwnershipAndUtf8Overflow() throws Exception {
        var valid = body("规范", "manual://utf8", "中".repeat(34_133), MapEntry.of("language", "zh-CN"));
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-utf8-boundary-ok")
                        .contentType(MediaType.APPLICATION_JSON).content(valid))
                .andExpect(status().isCreated());
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-utf8-boundary-too-large")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("规范", "manual://utf8", "中".repeat(34_134), MapEntry.of("language", "zh-CN"))))
                .andExpect(status().isBadRequest());
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-invalid-metadata")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bodyWithRawMetadata("规范", "manual://invalid", "正文", "{\"language\":123}")))
                .andExpect(status().isBadRequest());
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-forged-fields")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"title\":\"规范\",\"sourceRef\":\"manual://forged\",\"content\":\"正文\",\"ownerId\":\"80000000-0000-4000-8000-000000000002\",\"status\":\"PUBLISHED\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(body("规范", "manual://missing-key", "正文", MapEntry.of("language", "zh-CN"))))
                .andExpect(status().isBadRequest());
    }

    // 位置按 code point 返回，重复版本通过数据库唯一键收敛，不能切断 emoji。
    @Test
    void chunksDraftVersionDeterministicallyWithUnicodeOffsets() throws Exception {
        var content = "a".repeat(790) + "\n\n" + "😀".repeat(20) + "\n" + "b".repeat(20);
        var created = mvc.perform(post(path(WORKSPACE_A))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-chunk-boundary")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("切块规范", "manual://chunk", content, MapEntry.of("category", "policy"))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = mapper.readTree(created.getResponse().getContentAsString()).path("id").asText();

        var first = mvc.perform(post(path(WORKSPACE_A) + "/" + documentId + "/chunks")
                        .param("chunkingVersion", "p3-plain-1")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn();
        var firstChunks = mapper.readTree(first.getResponse().getContentAsString());
        assertThat(firstChunks).hasSize(2);
        assertThat(firstChunks.get(0).path("chunkOrder").asInt()).isEqualTo(1);
        assertThat(firstChunks.get(0).path("endOffset").asInt()).isEqualTo(792);
        assertThat(firstChunks.get(1).path("startOffset").asInt()).isEqualTo(792);
        assertThat(firstChunks.get(1).path("offsetUnit").asText()).isEqualTo("UNICODE_CODE_POINT");
        assertThat(firstChunks.get(1).path("content").asText()).startsWith("😀");

        mvc.perform(post(path(WORKSPACE_A) + "/" + documentId + "/chunks")
                        .param("chunkingVersion", "p3-plain-1")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "select count(*) from knowledge.chunk where document_id = ? and chunking_version = 'p3-plain-1'", Integer.class,
                UUID.fromString(documentId))).isEqualTo(2);

        var second = mvc.perform(post(path(WORKSPACE_A) + "/" + documentId + "/chunks")
                        .param("chunkingVersion", "p3-plain-2")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn();
        assertThat(mapper.readTree(second.getResponse().getContentAsString())).hasSize(3);
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "select count(distinct chunking_version) from knowledge.chunk where document_id = ?", Integer.class,
                UUID.fromString(documentId))).isEqualTo(2);
    }

    @Test
    void concurrentSameKeyCreatesOneDocument() throws Exception {
        var request = body("并发规范", "manual://concurrent", "同一份正文", MapEntry.of("category", "policy"));
        var start = new CountDownLatch(1);
        var calls = new ArrayList<Callable<MvcResult>>();
        for (var i = 0; i < 8; i++) {
            calls.add(() -> {
                start.await();
                return mvc.perform(post(path(WORKSPACE_A))
                                .header("Authorization", "Bearer eaf-local-alice")
                                .header("Idempotency-Key", "p3-import-concurrent")
                                .contentType(MediaType.APPLICATION_JSON).content(request))
                        .andReturn();
            });
        }
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = calls.stream().map(executor::submit).toList();
            start.countDown();
            var ids = futures.stream().map(future -> {
                try {
                    var result = future.get();
                    assertThat(result.getResponse().getStatus()).isEqualTo(201);
                    return mapper.readTree(result.getResponse().getContentAsString()).path("id").asText();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).distinct().toList();
            assertThat(ids).hasSize(1);
        }
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "select count(*) from knowledge.document where idempotency_key = 'p3-import-concurrent'", Integer.class)).isEqualTo(1);
    }

    private String path(String workspace) { return "/api/v1/workspaces/" + workspace + "/knowledge/documents"; }

    private String body(String title, String sourceRef, String content, MapEntry metadata) throws Exception {
        var map = new LinkedHashMap<String, Object>();
        map.put("title", title);
        map.put("sourceRef", sourceRef);
        map.put("content", content);
        map.put("metadata", Map.of(metadata.key(), metadata.value()));
        return mapper.writeValueAsString(map);
    }

    private String bodyWithRawMetadata(String title, String sourceRef, String content, String metadata) {
        return "{\"title\":\"" + title + "\",\"sourceRef\":\"" + sourceRef + "\",\"content\":\"" + content + "\",\"metadata\":" + metadata + "}";
    }

    private record MapEntry(String key, String value) {
        static MapEntry of(String key, String value) { return new MapEntry(key, value); }
    }
}
// 本测试覆盖真实 PostgreSQL 下的草稿导入、授权边界、UTF-8 字节限制和并发幂等。
