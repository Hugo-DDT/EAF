package io.eaf.bootstrap;

import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.knowledge.api.KnowledgeIndexBuild;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.model.api.EmbeddingRequest;
import io.eaf.model.api.EmbeddingResult;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
//  用真实 PostgreSQL/pgvector 验证不同空间隔离、失败回退、授权复查和持久恢复。
class P7KnowledgeIndexProfileTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE = "10000000-0000-4000-8000-000000000001";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired KnowledgeService knowledge;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean EmbeddingGateway embeddings;

    private final AtomicReference<EmbeddingProfile> activeProfile = new AtomicReference<>();
    private final AtomicInteger providerCalls = new AtomicInteger();
    private final AtomicInteger failAtCall = new AtomicInteger();
    private final AtomicInteger revokePermissionAtCall = new AtomicInteger();
    private final AtomicInteger blockAtCall = new AtomicInteger();
    private final List<EmbeddingRequest> requests = new CopyOnWriteArrayList<>();
    private volatile CountDownLatch blockedCallEntered;
    private volatile CountDownLatch releaseBlockedCall;

    @BeforeEach
    void installFixtureGatewayAndPermission() {
        activeProfile.set(profile(64, 10));
        providerCalls.set(0);
        failAtCall.set(0);
        revokePermissionAtCall.set(0);
        blockAtCall.set(0);
        requests.clear();
        blockedCallEntered = null;
        releaseBlockedCall = null;
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'knowledge:external-embedding', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        when(embeddings.profile()).thenAnswer(call -> activeProfile.get());
        when(embeddings.embed(any())).thenAnswer(call -> embedFixture(call.getArgument(0, EmbeddingRequest.class)));
    }

    @Test
    void storesDistinctDimensionsAndSearchesOnlyTheMatchingPublishedSpace() {
        var first = createDocument("exact profile sixty four");
        var firstBuild = build(first, "p3-plain-1", "profile-64-first");
        assertThat(firstBuild.status()).isEqualTo("READY");
        publish(first, firstBuild, "profile-64-publish");
        assertThat(jdbc.queryForObject("select vector_dims(embedding) from knowledge.embedding where build_id = ?",
                Integer.class, firstBuild.id())).isEqualTo(64);
        assertThat(jdbc.queryForObject("select max_batch_size from knowledge.index_build where id = ?", Integer.class,
                firstBuild.id())).isEqualTo(10);

        activeProfile.set(profile(128, 8));
        var second = createDocument("exact profile one hundred twenty eight");
        var secondBuild = build(second, "p3-plain-1", "profile-128-second");
        assertThat(secondBuild.status()).isEqualTo("READY");
        publish(second, secondBuild, "profile-128-publish");
        assertThat(jdbc.queryForList("select distinct dimension from knowledge.embedding order by dimension", Integer.class))
                .containsExactly(64, 128);
        assertSearchIncludesAndExcludes(second.id(), first.id(), "exact profile one hundred twenty eight");

        activeProfile.set(profile(64, 10));
        assertSearchIncludesAndExcludes(first.id(), second.id(), "exact profile sixty four");
    }

    @Test
    void failedNewProfileBuildLeavesThePreviousPublicationSearchableAfterRollback() {
        var original = createDocument("published profile sixty four remains available");
        var originalBuild = build(original, "p3-plain-1", "profile-fallback-original");
        publish(original, originalBuild, "profile-fallback-publish-original");

        var current = knowledge.get(ALICE, Ids.WORKSPACE_A, original.id());
        var revised = knowledge.createVersion(new CreateKnowledgeVersionCommand(ALICE, Ids.WORKSPACE_A,
                original.id(), current.rowVersion(), "new version waits for a safe index switch", "profile-fallback-v2"));
        knowledge.chunk(ALICE, Ids.WORKSPACE_A, original.id(), revised.version(), "p3-plain-1");
        activeProfile.set(profile(128, 8));
        failAtCall.set(providerCalls.get() + 1);
        var failed = knowledge.buildIndex(ALICE, Ids.WORKSPACE_A, original.id(), revised.version(),
                "p3-plain-1", "profile-fallback-new-profile");

        assertThat(failed.status()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select build_id from knowledge.document_publication where document_id = ? and status = 'ACTIVE'",
                UUID.class, original.id())).isEqualTo(originalBuild.id());
        assertThat(jdbc.queryForObject("select asset_version from knowledge.document_publication where document_id = ? and status = 'ACTIVE'",
                Integer.class, original.id())).isEqualTo(1);

        // 新 Profile 暂无对应的已发布向量；回滚到旧 Profile 后，旧指针仍能提供授权检索。
        activeProfile.set(profile(64, 10));
        assertSearchIncludes(original.id(), "published profile sixty four remains available");
    }

    @Test
    void sameIdempotencyKeyCannotSilentlyReuseAnotherProfileBuild() {
        var document = createDocument("profile key binding");
        var first = build(document, "p3-plain-1", "profile-fixed-idempotency-key");
        assertThat(first.status()).isEqualTo("READY");
        var callsBefore = providerCalls.get();
        activeProfile.set(profile(128, 8));

        assertThatThrownBy(() -> knowledge.buildIndex(ALICE, Ids.WORKSPACE_A, document.id(), 1,
                "p3-plain-1", "profile-fixed-idempotency-key"))
                .isInstanceOf(EafException.class)
                .satisfies(error -> assertThat(((EafException) error).code()).isEqualTo("IDEMPOTENCY_CONFLICT"));
        assertThat(providerCalls.get()).isEqualTo(callsBefore);
    }

    @Test
    void concurrentPublicationAllowsOnlyOneReadyProfileToWinTheDocumentVersion() throws Exception {
        var document = createDocument("concurrent profile publication");
        var firstBuild = build(document, "p3-plain-1", "profile-race-first");
        activeProfile.set(profile(128, 8));
        var secondBuild = build(document, "p3-plain-1", "profile-race-second");
        assertThat(firstBuild.status()).isEqualTo("READY");
        assertThat(secondBuild.status()).isEqualTo("READY");

        try (var executor = Executors.newFixedThreadPool(2)) {
            var start = new CountDownLatch(1);
            var first = executor.submit(() -> publishRace(document, firstBuild, "profile-race-publish-first", start));
            var second = executor.submit(() -> publishRace(document, secondBuild, "profile-race-publish-second", start));
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("PUBLISHED", "VERSION_CONFLICT");
        }
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_publication where document_id = ? and status = 'ACTIVE'",
                Integer.class, document.id())).isEqualTo(1);
    }

    @Test
    void permissionRevocationStopsTheNextEmbeddingBatch() {
        activeProfile.set(profile(64, 1));
        revokePermissionAtCall.set(1);
        var document = createDocument("permission batch ".repeat(450));
        var build = build(document, "p3-plain-2", "profile-permission-revoke");

        assertThat(build.status()).isEqualTo("FAILED");
        assertThat(providerCalls.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.embedding where build_id = ?", Integer.class, build.id()))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from workspace.\"grant\" where workspace_id = ? and actor_id = ? and action = 'knowledge:external-embedding'",
                String.class, Ids.WORKSPACE_A, Ids.ALICE)).isEqualTo("REVOKED");
    }

    @Test
    void staleBuildRecoveryUsesANewCallKeyAndOnlyOneConcurrentWorker() throws Exception {
        activeProfile.set(profile(64, 1));
        failAtCall.set(2);
        var document = createDocument("r".repeat(900));
        var failed = build(document, "p3-plain-2", "profile-crash-recovery");
        assertThat(failed.status()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.embedding where build_id = ?", Integer.class, failed.id()))
                .isEqualTo(1);

        // 模拟进程在批次调用间退出；首批向量已落库，索引行仍停在过期 INDEXING。
        jdbc.update("update knowledge.index_build set status = 'INDEXING', failure_code = null, updated_at = now() - interval '2 minutes' where id = ?", failed.id());
        failAtCall.set(0);
        blockedCallEntered = new CountDownLatch(1);
        releaseBlockedCall = new CountDownLatch(1);
        blockAtCall.set(3);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var recovery = executor.submit(() -> knowledge.retryIndex(ALICE, Ids.WORKSPACE_A, document.id(), failed.id()));
            assertThat(blockedCallEntered.await(5, TimeUnit.SECONDS)).isTrue();
            var competing = executor.submit(() -> knowledge.retryIndex(ALICE, Ids.WORKSPACE_A, document.id(), failed.id()));
            competing.get(5, TimeUnit.SECONDS);
            releaseBlockedCall.countDown();
            recovery.get(10, TimeUnit.SECONDS);
        } finally {
            releaseBlockedCall.countDown();
        }

        var recovered = knowledge.getIndexBuild(ALICE, Ids.WORKSPACE_A, document.id(), failed.id());
        assertThat(recovered.status()).isEqualTo("READY");
        assertThat(jdbc.queryForObject("select retry_generation from knowledge.index_build where id = ?", Integer.class, failed.id()))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.embedding where build_id = ?", Integer.class, failed.id()))
                .isEqualTo(3);
        assertThat(requests).extracting(request -> request.scope().callKey())
                .contains("job:embedding:" + failed.id() + ":attempt:0:start:1",
                        "job:embedding:" + failed.id() + ":attempt:0:start:2",
                        "job:embedding:" + failed.id() + ":attempt:1:start:2",
                        "job:embedding:" + failed.id() + ":attempt:1:start:3")
                .doesNotHaveDuplicates();
    }

    private EmbeddingResult embedFixture(EmbeddingRequest request) throws InterruptedException {
        requests.add(request);
        var callNumber = providerCalls.incrementAndGet();
        if (callNumber == revokePermissionAtCall.get())
            jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where workspace_id = ? and actor_id = ? and action = 'knowledge:external-embedding'",
                    Ids.WORKSPACE_A, Ids.ALICE);
        if (callNumber == blockAtCall.get()) {
            blockedCallEntered.countDown();
            if (!releaseBlockedCall.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("fixture embedding gate timed out");
        }
        if (callNumber == failAtCall.get()) throw new EmbeddingFailure("FIXTURE_UPSTREAM_FAILURE", "fixture failure", false, true);
        var dimension = request.dimension();
        var vectors = request.texts().stream().map(text -> fixtureVector(dimension, text)).toList();
        var tokens = request.texts().stream().mapToInt(text -> Math.max(1, (text.codePointCount(0, text.length()) + 3) / 4)).sum();
        var now = Instant.now();
        return new EmbeddingResult(request.provider(), request.model(), dimension, vectors, tokens, "KNOWN", now, now);
    }

    private List<Float> fixtureVector(int dimension, String text) {
        var vector = new ArrayList<Float>(java.util.Collections.nCopies(dimension, 0f));
        vector.set(Math.floorMod(text.hashCode(), dimension), 1f);
        return List.copyOf(vector);
    }

    private EmbeddingProfile profile(int dimension, int batchSize) {
        return new EmbeddingProfile("fixture", "fixture-embedding-" + dimension, "fixture-revision-1",
                dimension, batchSize, 8_192, 8_192);
    }

    private KnowledgeDocument createDocument(String content) {
        var suffix = UUID.randomUUID().toString();
        return knowledge.create(new CreateKnowledgeDocumentCommand(ALICE, Ids.WORKSPACE_A, "index profile",
                "manual://p7-profile/" + suffix, content, Map.of(), "p7-profile-document-" + suffix, "trace-p7-profile-" + suffix));
    }

    private KnowledgeIndexBuild build(KnowledgeDocument document, String chunkingVersion, String idempotencyKey) {
        knowledge.chunk(ALICE, Ids.WORKSPACE_A, document.id(), document.version(), chunkingVersion);
        return knowledge.buildIndex(ALICE, Ids.WORKSPACE_A, document.id(), document.version(), chunkingVersion, idempotencyKey);
    }

    private void publish(KnowledgeDocument document, KnowledgeIndexBuild build, String key) {
        knowledge.publish(ALICE, Ids.WORKSPACE_A, document.id(), document.rowVersion(), 0, build.id(), key);
    }

    private void assertSearchIncludes(UUID expectedDocument, String query) {
        var result = knowledge.search(ALICE, Ids.WORKSPACE_A, query, 5);
        assertThat(result.hits()).extracting(hit -> hit.documentId()).contains(expectedDocument);
    }

    private void assertSearchIncludesAndExcludes(UUID expectedDocument, UUID excludedDocument, String query) {
        var result = knowledge.search(ALICE, Ids.WORKSPACE_A, query, 5);
        assertThat(result.hits()).extracting(hit -> hit.documentId())
                .contains(expectedDocument)
                .doesNotContain(excludedDocument);
    }

    private String publishRace(KnowledgeDocument document, KnowledgeIndexBuild build, String key, CountDownLatch start) throws InterruptedException {
        start.await();
        try {
            knowledge.publish(ALICE, Ids.WORKSPACE_A, document.id(), document.rowVersion(), 0, build.id(), key);
            return "PUBLISHED";
        } catch (EafException conflict) {
            return conflict.code();
        }
    }
}
