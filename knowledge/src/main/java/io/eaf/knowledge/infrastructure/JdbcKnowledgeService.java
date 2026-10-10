package io.eaf.knowledge.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.CreateKnowledgeVersionCommand;
import io.eaf.knowledge.api.KnowledgeChunk;
import io.eaf.knowledge.api.KnowledgeDocument;
import io.eaf.knowledge.api.KnowledgeDocumentPage;
import io.eaf.knowledge.api.KnowledgeIndexBuild;
import io.eaf.knowledge.api.KnowledgePublication;
import io.eaf.knowledge.api.KnowledgeSearchHit;
import io.eaf.knowledge.api.KnowledgeSearchResult;
import io.eaf.knowledge.api.KnowledgeSearchScope;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.ContextShare;
import io.eaf.knowledge.api.ContextSharePage;
import io.eaf.knowledge.api.ScopedKnowledgeSearchHit;
import io.eaf.knowledge.api.ScopedKnowledgeSearchResult;
import io.eaf.knowledge.api.KnowledgeOutboxItem;
import io.eaf.knowledge.api.KnowledgeOutboxPage;
import io.eaf.knowledge.api.KnowledgeOutboxReplayReceipt;
import io.eaf.knowledge.api.ManagedKnowledgeSource;
import io.eaf.knowledge.api.PublishedKnowledgeChunk;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.model.api.EmbeddingCallScope;
import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.model.api.EmbeddingRequest;
import io.eaf.model.api.EmbeddingResult;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.identity.api.IdentityDirectory;
import io.eaf.identity.api.IdentityService;
import io.eaf.identity.api.DelegationResourceAuthorizer;
import io.eaf.identity.api.McpReadonlyDelegationScope;
import io.eaf.workspace.api.WorkspaceAccess;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceKind;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Autowired;

@Service
public class JdbcKnowledgeService implements KnowledgeService, DelegationResourceAuthorizer {
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "DELIVERED", "FAILED");
    private static final int MAX_CONTENT_BYTES = 100 * 1024;
    private static final int MAX_MANAGED_BATCH_ITEMS = 20;
    private static final int MAX_MANAGED_BATCH_BYTES = 1024 * 1024;
    private static final int MAX_PGVECTOR_DIMENSION = 16_000;
    private static final Pattern METADATA_KEY = Pattern.compile("[A-Za-z][A-Za-z0-9_.-]{0,63}");
    private static final Map<String, Integer> CHUNK_LIMITS = Map.of("p3-plain-1", 800, "p3-plain-2", 400, "p9-structure-1", 800);
    private static final Pattern MARKDOWN_HEADING = Pattern.compile("^ {0,3}(#{1,6})[\\t ]+(.+?)\\s*$");
    private static final Pattern MARKDOWN_FENCE = Pattern.compile("^ {0,3}(`{3,}|~{3,}).*$");
    private static final int EMBEDDING_INPUT_TOKENS = 8_192;
    private static final int SEARCH_TOP_K_MAX = 10;
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final WorkspaceCatalog workspaceCatalog;
    private final IdentityDirectory identities;
    private final IdentityService delegationIdentities;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final EmbeddingGateway embeddings;
    private final AuditPort audit;

    @Autowired
    public JdbcKnowledgeService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                WorkspaceCatalog workspaceCatalog, IdentityDirectory identities,
                                IdentityService delegationIdentities, ObjectMapper mapper, Clock clock,
                                EmbeddingGateway embeddings, AuditPort audit) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.workspaceCatalog = workspaceCatalog;
        this.identities = identities;
        this.delegationIdentities = delegationIdentities;
        this.mapper = mapper;
        this.clock = clock;
        this.embeddings = embeddings;
        this.audit = audit;
    }

    /** 保留既有测试/嵌入式组装方的单空间构造入口； 分享 API 仅由 Spring 主装配启用。 */
    public JdbcKnowledgeService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                ObjectMapper mapper, Clock clock, EmbeddingGateway embeddings, AuditPort audit) {
        this(jdbc, workspaces, null, null, null, mapper, clock, embeddings, audit);
    }

    @Override
    public boolean mayDelegateKnowledgeRead(UUID tenantId, UUID ownerId, UUID delegateId,
                                            UUID workspaceId, UUID documentId) {
        if (tenantId == null || ownerId == null || delegateId == null || workspaceId == null || documentId == null
                || !workspaces.isAuthorized(tenantId, ownerId, workspaceId, "knowledge:read")
                || !workspaces.isAuthorized(tenantId, delegateId, workspaceId, "knowledge:read")) return false;
        var count = jdbc.queryForObject("select count(*) from knowledge.document d "
                        + "join knowledge.document_publication p on p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.status = 'ACTIVE' "
                        + "join knowledge.document_version v on v.tenant_id = p.tenant_id and v.workspace_id = p.workspace_id and v.document_id = p.document_id and v.asset_version = p.asset_version and v.status = 'PUBLISHED' "
                        + "join knowledge.index_build b on b.id = p.build_id and b.tenant_id = p.tenant_id and b.workspace_id = p.workspace_id and b.document_id = p.document_id and b.asset_version = p.asset_version and b.status = 'READY' "
                        + "where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and d.status = 'PUBLISHED' "
                        + "and b.total_chunks = b.completed_chunks and b.completed_chunks = (select count(*) from knowledge.embedding e where e.build_id = b.id) "
                        + "and exists (select 1 from knowledge.document_permission p1 where p1.tenant_id = d.tenant_id and p1.workspace_id = d.workspace_id and p1.document_id = d.id and p1.actor_id = ? and p1.action = 'knowledge:read' and p1.status = 'ACTIVE') "
                        + "and exists (select 1 from knowledge.document_permission p2 where p2.tenant_id = d.tenant_id and p2.workspace_id = d.workspace_id and p2.document_id = d.id and p2.actor_id = ? and p2.action = 'knowledge:read' and p2.status = 'ACTIVE')"
                        + managedSourceReadGuard() + managedSourceReadGuard(),
                Integer.class, tenantId, workspaceId, documentId, ownerId, delegateId, ownerId, delegateId);
        return count != null && count == 1;
    }

    @Override
    @Transactional
    public KnowledgeDocument create(CreateKnowledgeDocumentCommand command) {
        validate(command);
        WorkspaceAccess access = workspaces.require(command.actor(), command.workspaceId(), "knowledge:write");
        var metadata = new TreeMap<>(command.metadata() == null ? Map.of() : command.metadata());
        var contentHash = Hashing.sha256(command.content());
        var requestHash = Hashing.sha256(String.join("\u001f", command.title(), command.sourceRef(),
                command.content(), metadataJson(metadata)));
        var now = Instant.now(clock);
        var documentId = UUID.randomUUID();
        // 数据库唯一约束负责并发收敛；插入结果为零时再比较请求摘要，避免依赖异常包装。
        var inserted = jdbc.update("insert into knowledge.document(id, tenant_id, workspace_id, owner_id, title, source_ref, metadata, status, idempotency_key, request_hash, row_version, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?::jsonb, 'DRAFT', ?, ?, 1, ?, ?) on conflict (tenant_id, workspace_id, owner_id, idempotency_key) do nothing",
                documentId, access.tenantId(), access.workspaceId(), command.actor().actorId(), command.title(),
                command.sourceRef(), metadataJson(metadata), command.idempotencyKey(), requestHash,
                Timestamp.from(now), Timestamp.from(now));
        if (inserted == 0) {
            // 重复请求仍重新走可见性检查，不能泄露已存在文档。
            var existing = findByIdempotency(command.actor(), access.workspaceId(), command.idempotencyKey())
                    .orElseThrow(() -> EafException.conflict("IDEMPOTENCY_CONFLICT", "幂等键已占用。"));
            if (!requestHash.equals(existing.requestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一幂等键对应了不同请求。");
            return get(command.actor(), access.workspaceId(), existing.id());
        }
        // 先写不可变原文版本，再写 owner 的最小读取授权；后续索引和发布只能引用这个版本。
        jdbc.update("insert into knowledge.document_version(id, tenant_id, workspace_id, document_id, asset_version, content, content_hash, status, created_at) values (?, ?, ?, ?, 1, ?, ?, 'DRAFT', ?)",
                UUID.randomUUID(), access.tenantId(), access.workspaceId(), documentId, command.content(), contentHash,
                Timestamp.from(now));
        // 文档所有者默认拥有发布动作；真正发布仍需同时通过 Workspace 授权和 READY 索引校验。
        jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status) values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE'), (?, ?, ?, ?, 'knowledge:write', 'ACTIVE'), (?, ?, ?, ?, 'knowledge:publish', 'ACTIVE')",
                access.tenantId(), access.workspaceId(), documentId, command.actor().actorId(),
                access.tenantId(), access.workspaceId(), documentId, command.actor().actorId(),
                access.tenantId(), access.workspaceId(), documentId, command.actor().actorId());
        return new KnowledgeDocument(documentId, access.tenantId(), access.workspaceId(), command.actor().actorId(),
                command.title(), command.sourceRef(), Map.copyOf(metadata), 1, command.content(), contentHash,
                "DRAFT", now, 1);
    }

    @Override
    @Transactional
    public ManagedKnowledgeSource createManagedSource(ManagedKnowledgeSource.CreateCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null)
            throw EafException.invalid("受管来源上下文不能为空。");
        requireDirectHuman(command.actor(), "受管来源管理");
        var access = workspaces.require(command.actor(), command.workspaceId(), "knowledge:source:manage");
        workspaces.require(command.actor(), command.workspaceId(), "knowledge:write");
        if (blank(command.name()) || command.name().strip().length() > 160 || hasControl(command.name())
                || !"MANAGED_TEXT_V1".equals(command.type()))
            throw EafException.invalid("来源名称或类型无效；首版仅支持 MANAGED_TEXT_V1。");
        requireIdempotencyKey(command.idempotencyKey());
        var name = command.name().strip();
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), access.workspaceId().toString(),
                command.actor().actorId().toString(), command.idempotencyKey()));
        var requestHash = Hashing.sha256(name + "\u001f" + command.type());
        var prior = managedSourceByCreateKey(access, command.actor().actorId(), keyHash);
        if (prior != null) {
            if (!requestHash.equals(prior.createRequestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "来源创建幂等键已用于不同请求。");
            return prior.source();
        }
        var now = Timestamp.from(Instant.now(clock));
        var sourceId = UUID.randomUUID();
        var inserted = jdbc.update("insert into knowledge.managed_source(id, tenant_id, workspace_id, owner_id, name, source_type, status, source_revision, create_key_hash, create_request_hash, created_at, updated_at) values (?, ?, ?, ?, ?, 'MANAGED_TEXT_V1', 'ACTIVE', 1, ?, ?, ?, ?) on conflict (tenant_id, workspace_id, owner_id, create_key_hash) do nothing",
                sourceId, access.tenantId(), access.workspaceId(), command.actor().actorId(), name, keyHash, requestHash, now, now);
        if (inserted == 0) {
            prior = managedSourceByCreateKey(access, command.actor().actorId(), keyHash);
            if (prior == null || !requestHash.equals(prior.createRequestHash()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "来源创建幂等键已被并发请求占用。");
            return prior.source();
        }
        return new ManagedKnowledgeSource(sourceId, access.tenantId(), access.workspaceId(), command.actor().actorId(),
                name, command.type(), "ACTIVE", 1, now.toInstant(), now.toInstant());
    }

    @Override
    public List<ManagedKnowledgeSource> listManagedSources(ActorContext actor, UUID workspaceId, int limit, int offset) {
        var access = requireSourceManager(actor, workspaceId);
        if (limit < 1 || limit > 50 || offset < 0 || offset > 100_000)
            throw EafException.invalid("来源分页参数无效。");
        return jdbc.query("select * from knowledge.managed_source where tenant_id = ? and workspace_id = ? and owner_id = ? order by created_at desc, id desc limit ? offset ?",
                (rs, row) -> mapManagedSource(rs), access.tenantId(), workspaceId, actor.actorId(), limit, offset);
    }

    @Override
    public ManagedKnowledgeSource getManagedSource(ActorContext actor, UUID workspaceId, UUID sourceId) {
        var access = requireSourceManager(actor, workspaceId);
        return managedSource(access.tenantId(), workspaceId, actor.actorId(), sourceId, false)
                .orElseThrow(EafException::notFound);
    }

    @Override
    @Transactional
    public ManagedKnowledgeSource.StateReceipt changeManagedSourceState(ActorContext actor, UUID workspaceId,
            UUID sourceId, long expectedSourceRevision, String status, String idempotencyKey) {
        var access = requireSourceManager(actor, workspaceId);
        if (sourceId == null || expectedSourceRevision < 1 || !("ACTIVE".equals(status) || "DISABLED".equals(status)))
            throw EafException.invalid("来源状态变更参数无效。");
        requireIdempotencyKey(idempotencyKey);
        var keyHash = sourceOperationKeyHash(access, sourceId, actor.actorId(), idempotencyKey);
        var requestHash = Hashing.sha256("STATE\u001f" + expectedSourceRevision + "\u001f" + status);
        var prior = managedSourceOperation(access.tenantId(), workspaceId, sourceId, keyHash);
        if (prior != null) return requireMatchingStateReplay(actor, requestHash, prior);
        var source = managedSource(access.tenantId(), workspaceId, actor.actorId(), sourceId, true)
                .orElseThrow(EafException::notFound);
        prior = managedSourceOperation(access.tenantId(), workspaceId, sourceId, keyHash);
        if (prior != null) return requireMatchingStateReplay(actor, requestHash, prior);
        if (source.sourceRevision() != expectedSourceRevision)
            throw EafException.conflict("SOURCE_REVISION_CONFLICT", "来源版本已变化，请重新读取后提交。");
        var revision = "ACTIVE".equals(source.status()) == "ACTIVE".equals(status)
                ? source.sourceRevision() : source.sourceRevision() + 1;
        var now = Timestamp.from(Instant.now(clock));
        if (revision != source.sourceRevision()) {
            jdbc.update("update knowledge.managed_source set status = ?, source_revision = ?, updated_at = ? where tenant_id = ? and workspace_id = ? and id = ? and source_revision = ?",
                    status, revision, now, access.tenantId(), workspaceId, sourceId, expectedSourceRevision);
        }
        var operationId = UUID.randomUUID();
        jdbc.update("insert into knowledge.managed_source_operation(id, tenant_id, workspace_id, source_id, actor_id, operation_type, request_key_hash, request_hash, previous_source_revision, source_revision, result_status, result_source_status, observed_at) values (?, ?, ?, ?, ?, 'STATE', ?, ?, ?, ?, 'APPLIED', ?, ?)",
                operationId, access.tenantId(), workspaceId, sourceId, actor.actorId(), keyHash, requestHash,
                source.sourceRevision(), revision, status, now);
        audit.append(new AuditFact("managed-knowledge-source-state:" + operationId, access.tenantId(), workspaceId,
                actor.actorId(), null, "KNOWLEDGE_SOURCE_STATE_CHANGED", "APPLIED",
                "{\"sourceId\":\"" + sourceId + "\",\"status\":\"" + status + "\",\"sourceRevision\":" + revision + "}", null));
        return new ManagedKnowledgeSource.StateReceipt(operationId, sourceId, status,
                source.sourceRevision(), revision, now.toInstant(), false);
    }

    @Override
    @Transactional
    public ManagedKnowledgeSource.SyncReceipt applyManagedSourceSync(ManagedKnowledgeSource.BatchCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.sourceId() == null)
            throw EafException.invalid("受管来源批次上下文不能为空。");
        var access = requireSourceManager(command.actor(), command.workspaceId());
        workspaces.require(command.actor(), command.workspaceId(), "knowledge:write");
        validateManagedBatch(command);
        var keyHash = sourceOperationKeyHash(access, command.sourceId(), command.actor().actorId(), command.idempotencyKey());
        var requestHash = managedBatchHash(command);
        var prior = managedSourceOperation(access.tenantId(), command.workspaceId(), command.sourceId(), keyHash);
        if (prior != null) return requireMatchingSyncReplay(command.actor(), requestHash, prior);
        var source = managedSource(access.tenantId(), command.workspaceId(), command.actor().actorId(), command.sourceId(), true)
                .orElseThrow(EafException::notFound);
        prior = managedSourceOperation(access.tenantId(), command.workspaceId(), command.sourceId(), keyHash);
        if (prior != null) return requireMatchingSyncReplay(command.actor(), requestHash, prior);
        if (source.sourceRevision() != command.expectedSourceRevision())
            throw EafException.conflict("SOURCE_REVISION_CONFLICT", "来源版本已变化，请重新读取后提交。");
        validateManagedReaders(access, command.workspaceId(), command.changes());
        var now = Timestamp.from(Instant.now(clock));
        var syncId = UUID.randomUUID();
        var nextRevision = source.sourceRevision() + 1;
        jdbc.update("insert into knowledge.managed_source_operation(id, tenant_id, workspace_id, source_id, actor_id, operation_type, request_key_hash, request_hash, previous_source_revision, source_revision, result_status, observed_at) values (?, ?, ?, ?, ?, 'SYNC', ?, ?, ?, ?, 'APPLIED', ?)",
                syncId, access.tenantId(), command.workspaceId(), command.sourceId(), command.actor().actorId(),
                keyHash, requestHash, source.sourceRevision(), nextRevision, now);
        var results = new ArrayList<ManagedKnowledgeSource.ChangeResult>();
        for (int position = 0; position < command.changes().size(); position++)
            results.add(applyManagedChange(command.actor(), access, command.sourceId(), syncId,
                    command.changes().get(position), position, now));
        if (jdbc.update("update knowledge.managed_source set source_revision = ?, updated_at = ? where tenant_id = ? and workspace_id = ? and id = ? and source_revision = ?",
                nextRevision, now, access.tenantId(), command.workspaceId(), command.sourceId(), source.sourceRevision()) != 1)
            throw EafException.conflict("SOURCE_REVISION_CONFLICT", "来源版本已变化，请重新读取后提交。");
        audit.append(new AuditFact("managed-knowledge-source-sync:" + syncId, access.tenantId(), command.workspaceId(),
                command.actor().actorId(), null, "KNOWLEDGE_SOURCE_SYNC_APPLIED", "APPLIED",
                "{\"sourceId\":\"" + command.sourceId() + "\",\"itemCount\":" + results.size()
                        + ",\"sourceRevision\":" + nextRevision + "}", null));
        return new ManagedKnowledgeSource.SyncReceipt(syncId, command.sourceId(), "APPLIED", source.sourceRevision(),
                nextRevision, now.toInstant(), List.copyOf(results), false);
    }

    @Override
    public ManagedKnowledgeSource.SyncReceipt getManagedSourceSync(ActorContext actor, UUID workspaceId,
            UUID sourceId, UUID syncId) {
        var access = requireSourceManager(actor, workspaceId);
        managedSource(access.tenantId(), workspaceId, actor.actorId(), sourceId, false).orElseThrow(EafException::notFound);
        var operation = jdbc.query("select * from knowledge.managed_source_operation where tenant_id = ? and workspace_id = ? and source_id = ? and id = ? and operation_type = 'SYNC'",
                rs -> rs.next() ? mapManagedOperation(rs) : null, access.tenantId(), workspaceId, sourceId, syncId);
        if (operation == null) throw EafException.notFound();
        return syncReceipt(operation, false);
    }

    @Override
    public List<ManagedKnowledgeSource.ItemSummary> listManagedSourceItems(ActorContext actor, UUID workspaceId,
            UUID sourceId, int limit, int offset) {
        var access = requireSourceManager(actor, workspaceId);
        managedSource(access.tenantId(), workspaceId, actor.actorId(), sourceId, false).orElseThrow(EafException::notFound);
        if (limit < 1 || limit > 50 || offset < 0 || offset > 100_000)
            throw EafException.invalid("来源条目分页参数无效。");
        return jdbc.query("select i.item_id, i.document_id, i.display_title, i.source_version, i.content_hash, i.acl_version, i.availability, i.reason_code, i.observed_at, (select max(v.document_version) from knowledge.managed_source_version v join knowledge.document_version dv on dv.tenant_id = v.tenant_id and dv.workspace_id = v.workspace_id and dv.document_id = v.document_id and dv.asset_version = v.document_version and dv.status = 'DRAFT' where v.tenant_id = i.tenant_id and v.workspace_id = i.workspace_id and v.document_id = i.document_id) draft_version from knowledge.managed_source_item i where i.tenant_id = ? and i.workspace_id = ? and i.source_id = ? order by i.item_id limit ? offset ?",
                (rs, row) -> new ManagedKnowledgeSource.ItemSummary(rs.getString("item_id"), rs.getObject("document_id", UUID.class),
                        rs.getString("display_title"), rs.getString("source_version"), rs.getString("content_hash"),
                        rs.getString("acl_version"), rs.getString("availability"), rs.getString("reason_code"),
                        rs.getTimestamp("observed_at").toInstant(), (Integer) rs.getObject("draft_version")),
                access.tenantId(), workspaceId, sourceId, limit, offset);
    }

    @Override
    public ManagedKnowledgeSource.SourceVersion getManagedSourceVersion(ActorContext actor, UUID workspaceId,
            UUID documentId, int documentVersion) {
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        if (documentId == null || documentVersion < 1) throw EafException.invalid("来源版本引用无效。");
        return jdbc.query("select sv.source_id, sv.item_id, sv.source_version, sv.content_hash, sv.acl_version, sv.acl_hash, sv.sync_id, sv.observed_at, i.source_version current_source_version, i.content_hash current_content_hash, i.availability, i.reason_code, s.status source_status from knowledge.managed_source_version sv join knowledge.managed_source_item i on i.tenant_id = sv.tenant_id and i.workspace_id = sv.workspace_id and i.source_id = sv.source_id and i.item_id = sv.item_id join knowledge.managed_source s on s.tenant_id = i.tenant_id and s.workspace_id = i.workspace_id and s.id = i.source_id where sv.tenant_id = ? and sv.workspace_id = ? and sv.document_id = ? and sv.document_version = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = sv.tenant_id and p.workspace_id = sv.workspace_id and p.document_id = sv.document_id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE') and exists (select 1 from knowledge.managed_source_reader r where r.tenant_id = sv.tenant_id and r.workspace_id = sv.workspace_id and r.source_id = sv.source_id and r.item_id = sv.item_id and r.actor_id = ?)",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ManagedKnowledgeSource.SourceVersion>>) rs ->
                        rs.next() ? Optional.of(mapManagedSourceVersion(rs)) : Optional.empty(),
                access.tenantId(), workspaceId, documentId, documentVersion, actor.actorId(), actor.actorId())
                .orElseThrow(EafException::notFound);
    }

    @Override
    public KnowledgeDocument getManagedSourceVersionForOwner(ActorContext actor, UUID workspaceId, UUID sourceId,
            String itemId, int documentVersion) {
        var access = requireSourceManager(actor, workspaceId);
        workspaces.require(actor, workspaceId, "knowledge:write");
        if (documentVersion < 1 || !validManagedItemId(itemId)) throw EafException.invalid("来源历史版本引用无效。");
        managedSource(access.tenantId(), workspaceId, actor.actorId(), sourceId, false).orElseThrow(EafException::notFound);
        var binding = jdbc.query("select document_id from knowledge.managed_source_version where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ? and document_version = ?",
                rs -> rs.next() ? rs.getObject("document_id", UUID.class) : null,
                access.tenantId(), workspaceId, sourceId, itemId, documentVersion);
        if (binding == null) throw EafException.notFound();
        return rawKnowledgeVersion(access, binding, documentVersion);
    }

    @Override
    public ManagedKnowledgeSource.Neighborhood neighborhood(ManagedKnowledgeSource.NeighborhoodRequest request) {
        if (request == null || request.actor() == null) throw EafException.invalid("邻段读取上下文不能为空。");
        requireDirectHuman(request.actor(), "Knowledge 邻段读取");
        var access = workspaces.require(request.actor(), request.workspaceId(), "knowledge:read");
        if (request.documentId() == null || request.buildId() == null || request.chunkId() == null
                || request.documentVersion() < 1 || request.tokenBudget() < 1 || request.tokenBudget() > 2000)
            throw EafException.invalid("邻段引用或 tokenBudget 无效。");
        var seed = jdbc.query("select c.id, c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text from knowledge.chunk c join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id join knowledge.document_version v on v.tenant_id = c.tenant_id and v.workspace_id = c.workspace_id and v.document_id = c.document_id and v.asset_version = c.asset_version join knowledge.index_build b on b.id = ? and b.tenant_id = c.tenant_id and b.workspace_id = c.workspace_id and b.document_id = c.document_id and b.asset_version = c.asset_version and b.chunking_version = c.chunking_version and b.status = 'READY' join knowledge.document_publication p on p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id and p.document_id = b.document_id and p.asset_version = b.asset_version and p.build_id = b.id and p.status = 'ACTIVE' where c.id = ? and c.document_id = ? and c.asset_version = ? and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')"
                        + managedSourceReadGuard(),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeChunk>>) rs ->
                        rs.next() ? Optional.of(mapChunk(rs)) : Optional.empty(), request.buildId(), request.chunkId(),
                request.documentId(), request.documentVersion(), request.actor().actorId(), request.actor().actorId())
                .orElseThrow(EafException::notFound);
        var seedTokens = estimatedTokens(seed.content());
        if (seedTokens > request.tokenBudget())
            throw EafException.conflict("NEIGHBORHOOD_BUDGET_TOO_SMALL", "tokenBudget 小于种子片段，不能截断来源正文。");
        var neighbors = jdbc.query("select c.id, c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text from knowledge.chunk c join knowledge.index_build b on b.id = ? and b.tenant_id = c.tenant_id and b.workspace_id = c.workspace_id and b.document_id = c.document_id and b.asset_version = c.asset_version and b.chunking_version = c.chunking_version and b.status = 'READY' where c.tenant_id = ? and c.workspace_id = ? and c.document_id = ? and c.asset_version = ? and c.chunking_version = ? and c.heading_path = ?::jsonb and c.chunk_order <> ? and exists (select 1 from knowledge.embedding e where e.build_id = b.id and e.chunk_id = c.id) order by c.chunk_order",
                (rs, row) -> mapChunk(rs), request.buildId(), access.tenantId(), request.workspaceId(),
                request.documentId(), request.documentVersion(), seed.chunkingVersion(), jsonString(seed.headingPath()), seed.chunkOrder());
        var previous = neighbors.stream().filter(c -> c.chunkOrder() < seed.chunkOrder()).max(Comparator.comparingInt(KnowledgeChunk::chunkOrder)).orElse(null);
        var next = neighbors.stream().filter(c -> c.chunkOrder() > seed.chunkOrder()).min(Comparator.comparingInt(KnowledgeChunk::chunkOrder)).orElse(null);
        var remaining = request.tokenBudget() - seedTokens;
        var items = new ArrayList<ManagedKnowledgeSource.NeighborhoodItem>();
        items.add(new ManagedKnowledgeSource.NeighborhoodItem(seed, "SEED", seedTokens));
        var omitted = new ArrayList<String>();
        for (var pair : List.of(new Neighbor("PREVIOUS", previous), new Neighbor("NEXT", next))) {
            if (pair.chunk() == null) omitted.add(pair.relation() + "_SECTION_BOUNDARY");
            else {
                var tokens = estimatedTokens(pair.chunk().content());
                if (tokens <= remaining) {
                    items.add(new ManagedKnowledgeSource.NeighborhoodItem(pair.chunk(), pair.relation(), tokens));
                    remaining -= tokens;
                } else omitted.add(pair.relation() + "_BUDGET");
            }
        }
        items.sort(Comparator.comparingInt(item -> item.chunk().chunkOrder()));
        return new ManagedKnowledgeSource.Neighborhood(request.documentId(), request.documentVersion(), request.buildId(),
                List.copyOf(items), List.copyOf(omitted));
    }

    private WorkspaceAccess requireSourceManager(ActorContext actor, UUID workspaceId) {
        requireDirectHuman(actor, "受管来源管理");
        return workspaces.require(actor, workspaceId, "knowledge:source:manage");
    }

    private void requireDirectHuman(ActorContext actor, String operation) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden(operation + "只接受本人直接操作的 HUMAN 身份。");
    }

    private void requireIdempotencyKey(String key) {
        if (blank(key) || key.length() > 200 || hasControl(key))
            throw EafException.invalid("Idempotency-Key 必须为 1-200 个可记录字符。");
    }

    private boolean validManagedItemId(String value) {
        return value != null && value.matches("[A-Za-z0-9._-]{1,120}");
    }

    private void validateManagedBatch(ManagedKnowledgeSource.BatchCommand command) {
        requireIdempotencyKey(command.idempotencyKey());
        if (command.expectedSourceRevision() < 1 || command.changes() == null || command.changes().isEmpty()
                || command.changes().size() > MAX_MANAGED_BATCH_ITEMS)
            throw EafException.invalid("来源批次 revision 或条目数量无效。");
        var seen = new HashSet<String>();
        long bytes = 0;
        for (var change : command.changes()) {
            if (change == null || !validManagedItemId(change.itemId()) || !seen.add(change.itemId()))
                throw EafException.invalid("来源批次 itemId 无效或重复。");
            switch (String.valueOf(change.changeType())) {
                case "UPSERT" -> {
                    if (!validSourceVersion(change.sourceVersion()) || blank(change.title()) || change.title().strip().length() > 200
                            || hasControl(change.title()) || !("MARKDOWN".equals(change.format()) || "PLAIN_TEXT".equals(change.format()))
                            || blank(change.content()) || change.content().getBytes(StandardCharsets.UTF_8).length > MAX_CONTENT_BYTES
                            || !validSourceVersion(change.aclVersion()) || change.readActorIds() == null)
                        throw EafException.invalid("UPSERT 必须提供有效正文、版本、标题和完整访问名单。");
                    bytes += change.content().getBytes(StandardCharsets.UTF_8).length;
                }
                case "DELETE" -> {
                    if (!validSourceVersion(change.sourceVersion())) throw EafException.invalid("DELETE 必须提供 sourceVersion。");
                }
                case "ACCESS" -> {
                    if (!validSourceVersion(change.aclVersion()) || change.readActorIds() == null)
                        throw EafException.invalid("ACCESS 必须提供 ACL 版本和完整访问名单。");
                }
                case "UNAVAILABLE" -> {
                    if (change.reasonCode() == null || !change.reasonCode().matches("[A-Z0-9_]{1,80}"))
                        throw EafException.invalid("UNAVAILABLE 必须提供有限 reasonCode。");
                }
                default -> throw EafException.invalid("不支持的来源变更类型。");
            }
            if (change.readActorIds() != null && change.readActorIds().stream().anyMatch(java.util.Objects::isNull))
                throw EafException.invalid("来源访问名单不能包含空主体。");
            if (change.readActorIds() != null && change.readActorIds().size() > 100)
                throw EafException.invalid("来源访问名单最多包含 100 个主体。");
        }
        if (bytes > MAX_MANAGED_BATCH_BYTES) throw EafException.invalid("来源批次正文总量超过 1 MiB 限制。");
    }

    private boolean validSourceVersion(String value) {
        return value != null && !value.isBlank() && value.length() <= 160 && !hasControl(value);
    }

    private void validateManagedReaders(WorkspaceAccess access, UUID workspaceId,
                                        List<ManagedKnowledgeSource.Change> changes) {
        for (var change : changes) {
            if (change.readActorIds() == null) continue;
            for (var readerId : change.readActorIds()) {
                if (readerId == null || !workspaces.isAuthorized(access.tenantId(), readerId, workspaceId, "knowledge:read"))
                    throw EafException.invalid("来源访问名单只能包含当前 Workspace 内已有 knowledge:read 的有效主体。");
            }
        }
    }

    private String managedBatchHash(ManagedKnowledgeSource.BatchCommand command) {
        var value = new StringBuilder();
        appendHashField(value, "SYNC");
        appendHashField(value, Long.toString(command.expectedSourceRevision()));
        for (var change : command.changes()) {
            appendHashField(value, change.changeType());
            appendHashField(value, change.itemId());
            appendHashField(value, change.sourceVersion());
            appendHashField(value, change.title());
            appendHashField(value, change.format());
            appendHashField(value, change.content());
            appendHashField(value, change.aclVersion());
            appendHashField(value, change.reasonCode());
            if (change.readActorIds() == null) appendHashField(value, null);
            else {
                appendHashField(value, Integer.toString(change.readActorIds().size()));
                change.readActorIds().stream().sorted().forEach(id -> appendHashField(value, id.toString()));
            }
        }
        return Hashing.sha256(value.toString());
    }

    private void appendHashField(StringBuilder target, String field) {
        if (field == null) target.append("-1:");
        else target.append(field.length()).append(':').append(field);
    }

    private String sourceOperationKeyHash(WorkspaceAccess access, UUID sourceId, UUID actorId, String key) {
        return Hashing.sha256(String.join("\u001f", access.tenantId().toString(), access.workspaceId().toString(),
                sourceId.toString(), actorId.toString(), key));
    }

    private ManagedSourceCreatePrior managedSourceByCreateKey(WorkspaceAccess access, UUID ownerId, String keyHash) {
        return jdbc.query("select * from knowledge.managed_source where tenant_id = ? and workspace_id = ? and owner_id = ? and create_key_hash = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ManagedSourceCreatePrior>>) rs -> rs.next()
                        ? Optional.of(new ManagedSourceCreatePrior(mapManagedSource(rs), rs.getString("create_request_hash")))
                        : Optional.empty(), access.tenantId(), access.workspaceId(), ownerId, keyHash).orElse(null);
    }

    private Optional<ManagedKnowledgeSource> managedSource(UUID tenantId, UUID workspaceId, UUID ownerId,
                                                            UUID sourceId, boolean forUpdate) {
        return jdbc.query("select * from knowledge.managed_source where tenant_id = ? and workspace_id = ? and owner_id = ? and id = ?"
                        + (forUpdate ? " for update" : ""),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ManagedKnowledgeSource>>) rs -> rs.next()
                        ? Optional.of(mapManagedSource(rs)) : Optional.empty(), tenantId, workspaceId, ownerId, sourceId);
    }

    private ManagedKnowledgeSource mapManagedSource(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ManagedKnowledgeSource(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("name"),
                rs.getString("source_type"), rs.getString("status"), rs.getLong("source_revision"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
    }

    private ManagedOperation managedSourceOperation(UUID tenantId, UUID workspaceId, UUID sourceId, String keyHash) {
        return jdbc.query("select * from knowledge.managed_source_operation where tenant_id = ? and workspace_id = ? and source_id = ? and request_key_hash = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<ManagedOperation>) rs -> rs.next() ? mapManagedOperation(rs) : null,
                tenantId, workspaceId, sourceId, keyHash);
    }

    private ManagedOperation mapManagedOperation(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ManagedOperation(rs.getObject("id", UUID.class), rs.getObject("source_id", UUID.class),
                rs.getObject("actor_id", UUID.class), rs.getString("operation_type"), rs.getString("request_hash"),
                rs.getLong("previous_source_revision"), rs.getLong("source_revision"),
                rs.getString("result_source_status"), rs.getTimestamp("observed_at").toInstant());
    }

    private ManagedKnowledgeSource.StateReceipt requireMatchingStateReplay(ActorContext actor, String requestHash,
                                                                            ManagedOperation prior) {
        if (!"STATE".equals(prior.type()) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("SOURCE_OPERATION_CONFLICT", "来源幂等键已用于不同操作。");
        return new ManagedKnowledgeSource.StateReceipt(prior.id(), prior.sourceId(), prior.sourceStatus(),
                prior.previousRevision(), prior.revision(), prior.observedAt(), true);
    }

    private ManagedKnowledgeSource.SyncReceipt requireMatchingSyncReplay(ActorContext actor, String requestHash,
                                                                          ManagedOperation prior) {
        if (!"SYNC".equals(prior.type()) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("SOURCE_OPERATION_CONFLICT", "来源幂等键已用于不同操作。");
        return syncReceipt(prior, true);
    }

    private ManagedKnowledgeSource.SyncReceipt syncReceipt(ManagedOperation operation, boolean replayed) {
        var items = jdbc.query("select * from knowledge.managed_source_sync_item where sync_id = ? order by position",
                (rs, row) -> new ManagedKnowledgeSource.ChangeResult(rs.getString("item_id"), rs.getString("change_type"),
                        rs.getString("result"), rs.getObject("document_id", UUID.class),
                        (Integer) rs.getObject("document_version"), rs.getString("source_version"),
                        rs.getString("availability"), rs.getString("reason_code")), operation.id());
        return new ManagedKnowledgeSource.SyncReceipt(operation.id(), operation.sourceId(), "APPLIED",
                operation.previousRevision(), operation.revision(), operation.observedAt(), List.copyOf(items), replayed);
    }

    private ManagedKnowledgeSource.SourceVersion mapManagedSourceVersion(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ManagedKnowledgeSource.SourceVersion(rs.getObject("source_id", UUID.class), rs.getString("item_id"),
                rs.getString("source_version"), rs.getString("content_hash"), rs.getString("acl_version"),
                rs.getString("acl_hash"), rs.getObject("sync_id", UUID.class), rs.getTimestamp("observed_at").toInstant(),
                rs.getString("current_source_version"), rs.getString("current_content_hash"), rs.getString("availability"),
                rs.getString("reason_code"), rs.getString("source_status"));
    }

    private ManagedKnowledgeSource.ChangeResult applyManagedChange(ActorContext actor, WorkspaceAccess access,
            UUID sourceId, UUID syncId, ManagedKnowledgeSource.Change change, int position, Timestamp now) {
        var item = managedSourceItem(access.tenantId(), access.workspaceId(), sourceId, change.itemId(), true).orElse(null);
        String type = change.changeType();
        if (item == null && !"UPSERT".equals(type)) throw EafException.notFound();
        UUID documentId;
        Integer documentVersion = null;
        String sourceVersion = item == null ? null : item.sourceVersion();
        String contentHash = item == null ? null : item.contentHash();
        String aclVersion = item == null ? null : item.aclVersion();
        String aclHash = item == null ? null : item.aclHash();
        String availability = item == null ? null : item.availability();
        String reasonCode = null;
        String result;
        if ("UPSERT".equals(type)) {
            contentHash = Hashing.sha256(change.content());
            aclHash = managedAclHash(change.readActorIds());
            validateNextSourceVersion(access, sourceId, change.itemId(), item, change.sourceVersion(), contentHash);
            validateNextAclVersion(access, sourceId, change.itemId(), item, change.aclVersion(), aclHash);
            if (item == null) {
                var sourceRef = "eaf-managed-text:" + sourceId + ":" + change.itemId();
                var created = create(new CreateKnowledgeDocumentCommand(actor, access.workspaceId(), change.title(),
                        sourceRef, change.content(), Map.of(), managedCreateKey(sourceId, change.itemId()),
                        "managed-source:" + syncId));
                documentId = created.id();
                sourceVersion = change.sourceVersion();
                aclVersion = change.aclVersion();
                availability = "AVAILABLE";
                result = "CREATED_DRAFT";
                jdbc.update("insert into knowledge.managed_source_item(tenant_id, workspace_id, source_id, item_id, document_id, display_title, source_version, content_hash, acl_version, acl_hash, availability, observed_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'AVAILABLE', ?)",
                        access.tenantId(), access.workspaceId(), sourceId, change.itemId(), documentId, change.title().strip(),
                        sourceVersion, contentHash, aclVersion, aclHash, now);
                replaceManagedSourceReaders(access, sourceId, change.itemId(), documentId, change.readActorIds());
                recordManagedSourceVersion(access, sourceId, change.itemId(), documentId, created.version(), sourceVersion,
                        contentHash, aclVersion, aclHash, syncId, now);
                documentVersion = created.version();
            } else {
                documentId = item.documentId();
                boolean contentChanged = !item.contentHash().equals(contentHash);
                boolean aclChanged = !item.aclHash().equals(aclHash);
                if (contentChanged) {
                    var state = lockDocumentForVersion(access, actor, documentId).orElseThrow(EafException::notFound);
                    var version = createKnowledgeVersion(new CreateKnowledgeVersionCommand(actor, access.workspaceId(),
                            documentId, state.rowVersion(), change.content(), managedVersionKey(sourceId, change.itemId(), change.sourceVersion())),
                            access, true);
                    documentVersion = version.version();
                    result = "UPDATED_DRAFT";
                } else {
                    documentVersion = currentManagedDocumentVersion(access, documentId, contentHash);
                    result = aclChanged ? "ACCESS_UPDATED" : "UNCHANGED";
                }
                sourceVersion = change.sourceVersion();
                aclVersion = change.aclVersion();
                availability = "AVAILABLE";
                jdbc.update("update knowledge.managed_source_item set display_title = ?, source_version = ?, content_hash = ?, acl_version = ?, acl_hash = ?, availability = 'AVAILABLE', reason_code = null, observed_at = ? where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?",
                        change.title().strip(), sourceVersion, contentHash, aclVersion, aclHash, now,
                        access.tenantId(), access.workspaceId(), sourceId, change.itemId());
                replaceManagedSourceReaders(access, sourceId, change.itemId(), documentId, change.readActorIds());
                if (contentChanged) recordManagedSourceVersion(access, sourceId, change.itemId(), documentId,
                        documentVersion, sourceVersion, contentHash, aclVersion, aclHash, syncId, now);
            }
        } else if ("DELETE".equals(type)) {
            validateNextDeleteVersion(access, sourceId, change.itemId(), item, change.sourceVersion());
            documentId = item.documentId();
            sourceVersion = change.sourceVersion();
            availability = "DELETED";
            result = "MARKED_DELETED";
            jdbc.update("update knowledge.managed_source_item set source_version = ?, availability = 'DELETED', reason_code = null, observed_at = ? where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?",
                    sourceVersion, now, access.tenantId(), access.workspaceId(), sourceId, change.itemId());
            documentVersion = currentManagedDocumentVersion(access, documentId, contentHash);
        } else if ("ACCESS".equals(type)) {
            aclHash = managedAclHash(change.readActorIds());
            validateNextAclVersion(access, sourceId, change.itemId(), item, change.aclVersion(), aclHash);
            documentId = item.documentId();
            aclVersion = change.aclVersion();
            availability = item.availability();
            reasonCode = item.reasonCode();
            result = item.aclHash().equals(aclHash) ? "UNCHANGED" : "ACCESS_UPDATED";
            jdbc.update("update knowledge.managed_source_item set acl_version = ?, acl_hash = ?, observed_at = ? where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?",
                    aclVersion, aclHash, now, access.tenantId(), access.workspaceId(), sourceId, change.itemId());
            replaceManagedSourceReaders(access, sourceId, change.itemId(), documentId, change.readActorIds());
            documentVersion = currentManagedDocumentVersion(access, documentId, contentHash);
        } else {
            documentId = item.documentId();
            availability = "UNAVAILABLE";
            reasonCode = change.reasonCode();
            result = "MARKED_UNAVAILABLE";
            jdbc.update("update knowledge.managed_source_item set availability = 'UNAVAILABLE', reason_code = ?, observed_at = ? where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?",
                    reasonCode, now, access.tenantId(), access.workspaceId(), sourceId, change.itemId());
            documentVersion = currentManagedDocumentVersion(access, documentId, contentHash);
        }
        // ACCESS/UNAVAILABLE 也是当时观察到的来源事实，保留其当前 sourceVersion 供批次追溯。
        var factSourceVersion = sourceVersion;
        jdbc.update("insert into knowledge.managed_source_sync_item(sync_id, position, item_id, change_type, result, document_id, document_version, source_version, content_hash, acl_version, acl_hash, availability, reason_code) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                syncId, position, change.itemId(), type, result, documentId, documentVersion, factSourceVersion,
                contentHash, aclVersion, aclHash, availability, reasonCode);
        return new ManagedKnowledgeSource.ChangeResult(change.itemId(), type, result, documentId, documentVersion,
                sourceVersion, availability, reasonCode);
    }

    private Optional<ManagedItem> managedSourceItem(UUID tenantId, UUID workspaceId, UUID sourceId,
            String itemId, boolean forUpdate) {
        return jdbc.query("select * from knowledge.managed_source_item where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?"
                        + (forUpdate ? " for update" : ""),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ManagedItem>>) rs -> rs.next()
                        ? Optional.of(new ManagedItem(rs.getObject("document_id", UUID.class), rs.getString("display_title"),
                                rs.getString("source_version"), rs.getString("content_hash"), rs.getString("acl_version"),
                                rs.getString("acl_hash"), rs.getString("availability"), rs.getString("reason_code")))
                        : Optional.empty(), tenantId, workspaceId, sourceId, itemId);
    }

    private String managedAclHash(Set<UUID> actorIds) {
        return Hashing.sha256(actorIds.stream().sorted().map(UUID::toString).collect(java.util.stream.Collectors.joining("\u001f")));
    }

    private void validateNextSourceVersion(WorkspaceAccess access, UUID sourceId, String itemId, ManagedItem item,
                                           String sourceVersion, String contentHash) {
        var latest = jdbc.query("select si.change_type from knowledge.managed_source_sync_item si join knowledge.managed_source_operation so on so.id = si.sync_id where so.source_id = ? and si.item_id = ? and si.source_version = ? order by so.source_revision desc, so.observed_at desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<String>) rs -> rs.next() ? rs.getString(1) : null,
                sourceId, itemId, sourceVersion);
        if (latest != null && (item == null || !sourceVersion.equals(item.sourceVersion()) || !"UPSERT".equals(latest)
                || !contentHash.equals(item.contentHash())))
            throw EafException.conflict("SOURCE_VERSION_CONFLICT", "sourceVersion 已用于其他事实或正文摘要。");
        if (item != null && sourceVersion.equals(item.sourceVersion()) && !contentHash.equals(item.contentHash()))
            throw EafException.conflict("SOURCE_VERSION_CONFLICT", "同一 sourceVersion 不能对应不同正文。");
    }

    private void validateNextDeleteVersion(WorkspaceAccess access, UUID sourceId, String itemId, ManagedItem item,
                                           String sourceVersion) {
        var latest = jdbc.query("select si.change_type from knowledge.managed_source_sync_item si join knowledge.managed_source_operation so on so.id = si.sync_id where so.source_id = ? and si.item_id = ? and si.source_version = ? order by so.source_revision desc, so.observed_at desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<String>) rs -> rs.next() ? rs.getString(1) : null,
                sourceId, itemId, sourceVersion);
        if (latest != null && (!sourceVersion.equals(item.sourceVersion()) || !"DELETE".equals(latest)))
            throw EafException.conflict("SOURCE_VERSION_CONFLICT", "DELETE 必须使用当前 head 未用过的新 sourceVersion。");
    }

    private void validateNextAclVersion(WorkspaceAccess access, UUID sourceId, String itemId, ManagedItem item,
                                        String aclVersion, String aclHash) {
        if (item != null && aclVersion.equals(item.aclVersion())) {
            if (!aclHash.equals(item.aclHash()))
                throw EafException.conflict("SOURCE_ACL_VERSION_CONFLICT", "同一 aclVersion 不能对应不同访问名单。");
            return;
        }
        var seen = jdbc.queryForObject("select count(*) from knowledge.managed_source_sync_item si join knowledge.managed_source_operation so on so.id = si.sync_id where so.source_id = ? and si.item_id = ? and si.acl_version = ?",
                Integer.class, sourceId, itemId, aclVersion);
        if (seen != null && seen > 0)
            throw EafException.conflict("SOURCE_ACL_VERSION_CONFLICT", "历史 aclVersion 不能重新作为来源 head。");
    }

    private void replaceManagedSourceReaders(WorkspaceAccess access, UUID sourceId, String itemId, UUID documentId,
                                            Set<UUID> actorIds) {
        var existing = new HashSet<>(jdbc.queryForList("select actor_id from knowledge.managed_source_reader where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ?",
                UUID.class, access.tenantId(), access.workspaceId(), sourceId, itemId));
        var next = actorIds == null ? Set.<UUID>of() : actorIds;
        for (var removed : existing) {
            if (next.contains(removed)) continue;
            jdbc.update("delete from knowledge.managed_source_reader where tenant_id = ? and workspace_id = ? and source_id = ? and item_id = ? and actor_id = ?",
                    access.tenantId(), access.workspaceId(), sourceId, itemId, removed);
            jdbc.update("update knowledge.document_permission set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and document_id = ? and actor_id = ? and action = 'knowledge:read' and grant_origin = 'MANAGED_SOURCE'",
                    access.tenantId(), access.workspaceId(), documentId, removed);
        }
        for (var reader : next) {
            jdbc.update("insert into knowledge.managed_source_reader(tenant_id, workspace_id, source_id, item_id, actor_id) values (?, ?, ?, ?, ?) on conflict do nothing",
                    access.tenantId(), access.workspaceId(), sourceId, itemId, reader);
            jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status, grant_origin) values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE', 'MANAGED_SOURCE') on conflict (tenant_id, document_id, actor_id, action) do update set status = 'ACTIVE', grant_origin = case when knowledge.document_permission.status = 'ACTIVE' and knowledge.document_permission.grant_origin <> 'MANAGED_SOURCE' then knowledge.document_permission.grant_origin else 'MANAGED_SOURCE' end, workspace_id = excluded.workspace_id",
                    access.tenantId(), access.workspaceId(), documentId, reader);
        }
    }

    private void recordManagedSourceVersion(WorkspaceAccess access, UUID sourceId, String itemId, UUID documentId,
            int documentVersion, String sourceVersion, String contentHash, String aclVersion, String aclHash,
            UUID syncId, Timestamp now) {
        jdbc.update("insert into knowledge.managed_source_version(tenant_id, workspace_id, source_id, item_id, document_id, document_version, source_version, content_hash, acl_version, acl_hash, sync_id, observed_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                access.tenantId(), access.workspaceId(), sourceId, itemId, documentId, documentVersion,
                sourceVersion, contentHash, aclVersion, aclHash, syncId, now);
    }

    private Integer currentManagedDocumentVersion(WorkspaceAccess access, UUID documentId, String contentHash) {
        return jdbc.query("select max(asset_version) from knowledge.document_version where tenant_id = ? and workspace_id = ? and document_id = ? and content_hash = ? and status <> 'REVOKED'",
                rs -> rs.next() ? (Integer) rs.getObject(1) : null, access.tenantId(), access.workspaceId(), documentId, contentHash);
    }

    private String managedCreateKey(UUID sourceId, String itemId) {
        return "managed-source-v1:" + sourceId + ":" + Hashing.sha256(itemId).substring(0, 16);
    }

    private String managedVersionKey(UUID sourceId, String itemId, String sourceVersion) {
        return "managed-source-v1:" + sourceId + ":" + Hashing.sha256(itemId).substring(0, 12)
                + ":" + Hashing.sha256(sourceVersion).substring(0, 16);
    }

    private KnowledgeDocument rawKnowledgeVersion(WorkspaceAccess access, UUID documentId, int version) {
        return jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.title, d.source_ref, d.metadata::text, v.asset_version, v.content, v.content_hash, v.status, d.created_at, d.row_version from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and v.asset_version = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeDocument>>) rs -> rs.next()
                        ? Optional.of(map(rs)) : Optional.empty(), access.tenantId(), access.workspaceId(), documentId, version)
                .orElseThrow(EafException::notFound);
    }

    private String managedSourceReadGuard() {
        return " and not exists (select 1 from knowledge.managed_source_item msi "
                + "join knowledge.managed_source ms on ms.tenant_id = msi.tenant_id and ms.workspace_id = msi.workspace_id and ms.id = msi.source_id "
                + "where msi.tenant_id = d.tenant_id and msi.workspace_id = d.workspace_id and msi.document_id = d.id "
                + "and (ms.status <> 'ACTIVE' or msi.availability <> 'AVAILABLE' or d.status <> 'PUBLISHED' "
                + "or v.status <> 'PUBLISHED' or coalesce(msi.content_hash, '') <> v.content_hash "
                + "or not exists (select 1 from knowledge.managed_source_reader r where r.tenant_id = msi.tenant_id "
                + "and r.workspace_id = msi.workspace_id and r.source_id = msi.source_id and r.item_id = msi.item_id and r.actor_id = ?)))";
    }

    private KnowledgeChunk mapChunk(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new KnowledgeChunk(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                rs.getObject("workspace_id", UUID.class), rs.getObject("document_id", UUID.class),
                rs.getInt("asset_version"), rs.getString("source_ref"), rs.getString("chunking_version"),
                rs.getInt("chunk_order"), rs.getInt("start_offset"), rs.getInt("end_offset"),
                rs.getString("offset_unit"), rs.getString("content"), rs.getString("content_hash"),
                stringList(rs.getString("heading_path")));
    }

    private int estimatedTokens(String text) {
        int codePoints = text.codePointCount(0, text.length());
        return Math.max(1, (codePoints + 3) / 4);
    }

    private boolean isManagedSourceDocument(WorkspaceAccess access, UUID documentId) {
        var count = jdbc.queryForObject("select count(*) from knowledge.managed_source_item where tenant_id = ? and workspace_id = ? and document_id = ?",
                Integer.class, access.tenantId(), access.workspaceId(), documentId);
        return count != null && count > 0;
    }

    private boolean isManagedSourceOwner(ActorContext actor, UUID workspaceId, UUID documentId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated()) return false;
        var count = jdbc.queryForObject("select count(*) from knowledge.managed_source_item i join knowledge.managed_source s on s.tenant_id = i.tenant_id and s.workspace_id = i.workspace_id and s.id = i.source_id where i.tenant_id = ? and i.workspace_id = ? and i.document_id = ? and s.owner_id = ?",
                Integer.class, actor.tenantId(), workspaceId, documentId, actor.actorId());
        return count != null && count > 0;
    }

    private void requireCurrentManagedSourceVersion(ActorContext actor, WorkspaceAccess access,
            UUID documentId, int documentVersion) {
        var source = jdbc.query("select s.owner_id, s.status, i.availability, i.content_hash, v.content_hash version_hash from knowledge.managed_source_item i join knowledge.managed_source s on s.tenant_id = i.tenant_id and s.workspace_id = i.workspace_id and s.id = i.source_id join knowledge.document_version v on v.tenant_id = i.tenant_id and v.workspace_id = i.workspace_id and v.document_id = i.document_id and v.asset_version = ? where i.tenant_id = ? and i.workspace_id = ? and i.document_id = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<ManagedSourceVersionGate>) rs -> rs.next()
                        ? new ManagedSourceVersionGate(rs.getObject("owner_id", UUID.class), rs.getString("status"),
                                rs.getString("availability"), rs.getString("content_hash"), rs.getString("version_hash"))
                        : null, documentVersion, access.tenantId(), access.workspaceId(), documentId);
        if (source == null) return;
        if (actor.type() != ActorType.HUMAN || actor.delegated() || !actor.actorId().equals(source.ownerId()))
            throw EafException.notFound();
        if (!"ACTIVE".equals(source.status())) throw EafException.conflict("SOURCE_DISABLED", "受管来源已停用，不能索引或发布。");
        if (!"AVAILABLE".equals(source.availability()))
            throw EafException.conflict("SOURCE_UNAVAILABLE", "受管来源当前不可用，不能索引或发布。");
        if (!source.contentHash().equals(source.versionHash()))
            throw EafException.conflict("SOURCE_CONTENT_CHANGED", "知识版本不再匹配来源当前正文，不能索引或发布。");
    }

    private record ManagedSourceCreatePrior(ManagedKnowledgeSource source, String createRequestHash) { }
    private record ManagedOperation(UUID id, UUID sourceId, UUID actorId, String type, String requestHash,
                                    long previousRevision, long revision, String sourceStatus, Instant observedAt) { }
    private record ManagedItem(UUID documentId, String title, String sourceVersion, String contentHash,
                               String aclVersion, String aclHash, String availability, String reasonCode) { }
    private record ManagedSourceVersionGate(UUID ownerId, String status, String availability,
                                            String contentHash, String versionHash) { }
    private record Neighbor(String relation, KnowledgeChunk chunk) { }

    // 对文档加行锁后 CAS 递增 rowVersion，防止两个编辑基于同一旧版本覆盖顺序。
    @Override
    @Transactional
    public KnowledgeDocument createVersion(CreateKnowledgeVersionCommand command) {
        return createKnowledgeVersion(command, null, false);
    }

    private KnowledgeDocument createKnowledgeVersion(CreateKnowledgeVersionCommand command, WorkspaceAccess managedAccess,
                                                      boolean managedSourceWrite) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.documentId() == null
                || blank(command.content()) || command.content().getBytes(StandardCharsets.UTF_8).length > MAX_CONTENT_BYTES
                || blank(command.idempotencyKey()) || command.idempotencyKey().length() > 200 || hasControl(command.idempotencyKey()))
            throw EafException.invalid("新知识版本需要正文、范围和有效幂等键。");
        var access = managedAccess == null
                ? workspaces.require(command.actor(), command.workspaceId(), "knowledge:write") : managedAccess;
        if (!managedSourceWrite && isManagedSourceDocument(access, command.documentId()))
            throw EafException.conflict("MANAGED_SOURCE_VERSION_REQUIRED", "受管来源正文只能通过来源变更批次追加版本。");
        var requestHash = Hashing.sha256("KNOWLEDGE_VERSION|" + command.content());
        var prior = findVersionByIdempotency(access, command.documentId(), command.idempotencyKey());
        if (prior != null) {
            if (!requestHash.equals(prior.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "版本幂等键已用于不同正文。");
            return managedSourceWrite ? rawKnowledgeVersion(access, command.documentId(), prior.assetVersion())
                    : getVersion(command.actor(), command.workspaceId(), command.documentId(), prior.assetVersion());
        }
        var document = lockDocumentForVersion(access, command.actor(), command.documentId()).orElseThrow(EafException::notFound);
        // 锁后再次读同键事实，收敛两个并发的相同版本请求。
        prior = findVersionByIdempotency(access, command.documentId(), command.idempotencyKey());
        if (prior != null) {
            if (!requestHash.equals(prior.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "版本幂等键已用于不同正文。");
            return managedSourceWrite ? rawKnowledgeVersion(access, command.documentId(), prior.assetVersion())
                    : getVersion(command.actor(), command.workspaceId(), command.documentId(), prior.assetVersion());
        }
        if (document.rowVersion() != command.expectedRowVersion())
            throw EafException.conflict("VERSION_CONFLICT", "文档基线已变化，请重新读取后创建版本。");
        if (!List.of("DRAFT", "PUBLISHED").contains(document.status()))
            throw EafException.conflict("DOCUMENT_STATE_CONFLICT", "只有可编辑文档能创建新版本。");
        var assetVersion = jdbc.queryForObject("select coalesce(max(asset_version), 0) + 1 from knowledge.document_version where tenant_id = ? and workspace_id = ? and document_id = ?",
                Integer.class, access.tenantId(), access.workspaceId(), command.documentId());
        var contentHash = Hashing.sha256(command.content());
        var now = Timestamp.from(Instant.now(clock));
        jdbc.update("insert into knowledge.document_version(id, tenant_id, workspace_id, document_id, asset_version, content, content_hash, status, created_at, idempotency_key, request_hash) values (?, ?, ?, ?, ?, ?, ?, 'DRAFT', ?, ?, ?)",
                UUID.randomUUID(), access.tenantId(), access.workspaceId(), command.documentId(), assetVersion,
                command.content(), contentHash, now, command.idempotencyKey(), requestHash);
        var nextVersion = command.expectedRowVersion() + 1;
        if (jdbc.update("update knowledge.document set row_version = ?, updated_at = ? where tenant_id = ? and workspace_id = ? and id = ? and row_version = ?",
                nextVersion, now, access.tenantId(), access.workspaceId(), command.documentId(), command.expectedRowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "文档基线已变化，请重新读取后创建版本。");
        return managedSourceWrite ? rawKnowledgeVersion(access, command.documentId(), assetVersion)
                : getVersion(command.actor(), command.workspaceId(), command.documentId(), assetVersion);
    }

    @Override
    public KnowledgeDocument get(ActorContext actor, UUID workspaceId, UUID documentId) {
        // 知识详情是资源枚举边界；没有 knowledge:read 时统一返回 404，避免新增审批角色改变历史脱敏语义。
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read")) {
            throw EafException.notFound();
        }
        return jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.title, d.source_ref, d.metadata::text, v.asset_version, v.content, v.content_hash, v.status, d.created_at, d.row_version from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = coalesce((select p.asset_version from knowledge.document_publication p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.status = 'ACTIVE'), (select max(draft.asset_version) from knowledge.document_version draft where draft.tenant_id = d.tenant_id and draft.workspace_id = d.workspace_id and draft.document_id = d.id and draft.status <> 'REVOKED')) join knowledge.document_permission p on p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE' where d.id = ? and d.tenant_id = ? and d.workspace_id = ?"
                        + managedSourceReadGuard(),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeDocument>>) rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(), actor.actorId(), documentId, actor.tenantId(), workspaceId, actor.actorId())
                        .orElseThrow(EafException::notFound);
    }

    @Override
    public KnowledgeDocumentPage listDocuments(ActorContext actor, UUID workspaceId, Instant cursorCreatedAt,
                                               UUID cursorId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        if (pageSize < 1 || pageSize > 50 || (cursorCreatedAt == null) != (cursorId == null))
            throw EafException.invalid("Knowledge 文档列表分页参数无效。");
        var where = " from knowledge.document d where d.tenant_id = ? and d.workspace_id = ? "
                + "and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id "
                + "and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? "
                + "and p.action = 'knowledge:read' and p.status = 'ACTIVE')"
                + " and not exists (select 1 from knowledge.managed_source_item msi join knowledge.managed_source ms "
                + "on ms.tenant_id = msi.tenant_id and ms.workspace_id = msi.workspace_id and ms.id = msi.source_id "
                + "where msi.tenant_id = d.tenant_id and msi.workspace_id = d.workspace_id and msi.document_id = d.id "
                + "and (ms.status <> 'ACTIVE' or msi.availability <> 'AVAILABLE' or not exists (select 1 from knowledge.document_publication p "
                + "join knowledge.document_version v on v.tenant_id = p.tenant_id and v.workspace_id = p.workspace_id "
                + "and v.document_id = p.document_id and v.asset_version = p.asset_version and v.status = 'PUBLISHED' "
                + "where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id "
                + "and p.status = 'ACTIVE' and v.content_hash = msi.content_hash) or not exists (select 1 from knowledge.managed_source_reader r "
                + "where r.tenant_id = msi.tenant_id and r.workspace_id = msi.workspace_id and r.source_id = msi.source_id "
                + "and r.item_id = msi.item_id and r.actor_id = ?)))";
        var total = jdbc.queryForObject("select count(*)" + where, Long.class,
                access.tenantId(), workspaceId, actor.actorId(), actor.actorId());
        var args = new java.util.ArrayList<Object>(List.of(access.tenantId(), workspaceId, actor.actorId(), actor.actorId()));
        var pageWhere = new StringBuilder(where);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (d.created_at, d.id) < (?, ?)");
            args.add(Timestamp.from(cursorCreatedAt));
            args.add(cursorId);
        }
        args.add(pageSize + 1);
        var selected = jdbc.query("select d.id, d.title, d.status, "
                        + "coalesce((select p.asset_version from knowledge.document_publication p where p.tenant_id = d.tenant_id "
                        + "and p.workspace_id = d.workspace_id and p.document_id = d.id and p.status = 'ACTIVE'), "
                        + "(select max(v.asset_version) from knowledge.document_version v where v.tenant_id = d.tenant_id "
                        + "and v.workspace_id = d.workspace_id and v.document_id = d.id and v.status <> 'REVOKED')) version, d.created_at"
                        + pageWhere + " order by d.created_at desc, d.id desc limit ?",
                (rs, row) -> new KnowledgeDocumentPage.Item(rs.getObject("id", UUID.class), rs.getString("title"),
                        rs.getString("status"), (Integer) rs.getObject("version"), rs.getTimestamp("created_at").toInstant()),
                args.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new KnowledgeDocumentPage(items, total == null ? 0 : total,
                last == null ? null : last.createdAt(), last == null ? null : last.id());
    }

    @Override
    public KnowledgeOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                                    Instant createdAfter, Instant cursorCreatedAt,
                                                    UUID cursorEventId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        if (pageSize < 1 || pageSize > 100 || (cursorCreatedAt == null) != (cursorEventId == null)
                || statuses != null && !OUTBOX_STATUSES.containsAll(statuses))
            throw EafException.invalid("Knowledge Outbox 运维分页或状态过滤无效。");
        var where = new StringBuilder(" where e.tenant_id = ? and e.workspace_id = ? "
                + "and exists (select 1 from knowledge.document_permission p where p.tenant_id = e.tenant_id "
                + "and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.actor_id = ? "
                + "and p.action = 'knowledge:read' and p.status = 'ACTIVE')");
        var filters = new ArrayList<Object>();
        filters.add(access.tenantId());
        filters.add(workspaceId);
        filters.add(actor.actorId());
        if (statuses != null) {
            if (statuses.isEmpty()) where.append(" and 1 = 0");
            else {
                where.append(" and o.status in (").append(String.join(",", java.util.Collections.nCopies(statuses.size(), "?"))).append(')');
                statuses.forEach(filters::add);
            }
        }
        if (createdAfter != null) {
            where.append(" and o.created_at >= ?");
            filters.add(Timestamp.from(createdAfter));
        }
        var from = " from knowledge.outbox o join knowledge.publication_event e on e.id = o.event_id";
        var totalSize = jdbc.queryForObject("select count(*)" + from + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new ArrayList<>(filters);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (o.created_at, o.event_id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursorCreatedAt));
            pageArgs.add(cursorEventId);
        }
        pageArgs.add(pageSize + 1);
        // 只通过本域发布事实补齐文档关联，绝不把知识正文或事件 payload 返回给运维端。
        var selected = jdbc.query("select o.event_id, e.document_id, e.asset_version, e.action, o.event_type, o.status, o.attempt_count, o.last_error, o.next_attempt_at, o.created_at"
                        + from + pageWhere + " order by o.created_at desc, o.event_id desc limit ?",
                (rs, row) -> new KnowledgeOutboxItem(rs.getObject("event_id", UUID.class),
                        rs.getObject("document_id", UUID.class), rs.getInt("asset_version"), rs.getString("action"),
                        rs.getString("event_type"), rs.getString("status"), rs.getInt("attempt_count"),
                        rs.getString("last_error"), rs.getTimestamp("next_attempt_at") == null ? null : rs.getTimestamp("next_attempt_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new KnowledgeOutboxPage(items, totalSize == null ? 0 : totalSize,
                last == null ? null : last.createdAt(), last == null ? null : last.eventId());
    }

    @Override
    @Transactional
    public KnowledgeOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                                     String requestKey, String reason) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Knowledge Outbox 重投只允许本人 HUMAN 操作者执行。");
        if (eventId == null || requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("Knowledge Outbox 重投需要事件、请求键和 1 至 500 字符的原因。");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        workspaces.require(actor, workspaceId, "knowledge:outbox:replay");
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), eventId.toString(), normalizedReason));

        // 文档级读取权限与 Outbox Owner 查询相同；FOR UPDATE 把人工重置与发布器投递串行化。
        var target = jdbc.query("select o.status, o.attempt_count from knowledge.outbox o "
                        + "join knowledge.publication_event e on e.id = o.event_id "
                        + "where o.event_id = ? and e.tenant_id = ? and e.workspace_id = ? "
                        + "and exists (select 1 from knowledge.document_permission p where p.tenant_id = e.tenant_id "
                        + "and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.actor_id = ? "
                        + "and p.action = 'knowledge:read' and p.status = 'ACTIVE') for update of o",
                rs -> rs.next() ? new ReplayTarget(rs.getString("status"), rs.getInt("attempt_count")) : null,
                eventId, access.tenantId(), workspaceId, actor.actorId());
        if (target == null) throw EafException.notFound();
        var prior = knowledgeReplayCommand(access.tenantId(), workspaceId, keyHash);
        if (prior != null) return requireMatchingKnowledgeReplay(actor, eventId, requestHash, prior);
        if (!"FAILED".equals(target.status()))
            throw EafException.conflict("OUTBOX_NOT_FAILED", "只有 FAILED 的 Knowledge Outbox 事件可重投。");

        var commandId = UUID.randomUUID();
        var now = Instant.now(clock);
        var inserted = jdbc.update("insert into knowledge.outbox_replay_command(command_id, tenant_id, workspace_id, event_id, "
                        + "actor_id, request_key_hash, request_hash, reason, result_status, attempts, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?) "
                        + "on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                commandId, access.tenantId(), workspaceId, eventId, actor.actorId(), keyHash, requestHash,
                normalizedReason, target.attempts(), Timestamp.from(now));
        if (inserted == 0) {
            prior = knowledgeReplayCommand(access.tenantId(), workspaceId, keyHash);
            if (prior == null) throw EafException.conflict("KNOWLEDGE_COMMAND_CONFLICT", "Knowledge Outbox 请求键已被并发命令占用。");
            return requireMatchingKnowledgeReplay(actor, eventId, requestHash, prior);
        }
        jdbc.update("update knowledge.outbox set status = 'PENDING', next_attempt_at = now() where event_id = ? and status = 'FAILED'", eventId);
        audit.append(new AuditFact("knowledge-outbox-replay:" + commandId, access.tenantId(), workspaceId,
                actor.actorId(), null, "KNOWLEDGE_OUTBOX_REPLAY_REQUESTED", "PENDING",
                "{\"eventId\":\"" + eventId + "\",\"attempts\":" + target.attempts()
                        + ",\"reasonHash\":\"" + Hashing.sha256(normalizedReason) + "\"}", null));
        return new KnowledgeOutboxReplayReceipt(commandId, eventId, "PENDING", target.attempts(), now, false);
    }

    private KnowledgeReplayCommand knowledgeReplayCommand(UUID tenantId, UUID workspaceId, String keyHash) {
        return jdbc.query("select command_id, event_id, actor_id, request_hash, result_status, attempts, created_at "
                        + "from knowledge.outbox_replay_command where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new KnowledgeReplayCommand(rs.getObject("command_id", UUID.class),
                        rs.getObject("event_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("request_hash"), rs.getString("result_status"), rs.getInt("attempts"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                tenantId, workspaceId, keyHash);
    }

    private KnowledgeOutboxReplayReceipt requireMatchingKnowledgeReplay(ActorContext actor, UUID eventId,
                                                                       String requestHash, KnowledgeReplayCommand prior) {
        if (!prior.eventId().equals(eventId) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("KNOWLEDGE_COMMAND_CONFLICT", "同一 Knowledge 重投请求键不能绑定不同事件、操作者或原因。");
        return new KnowledgeOutboxReplayReceipt(prior.commandId(), prior.eventId(), prior.status(),
                prior.attempts(), prior.createdAt(), true);
    }

    @Override
    public KnowledgeDocument getVersion(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion) {
        if (assetVersion < 1) throw EafException.invalid("assetVersion 必须为正数。");
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read")) throw EafException.notFound();
        return jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.title, d.source_ref, d.metadata::text, v.asset_version, v.content, v.content_hash, v.status, d.created_at, d.row_version from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id join knowledge.document_permission p on p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE' where d.id = ? and d.tenant_id = ? and d.workspace_id = ? and v.asset_version = ?"
                        + managedSourceReadGuard(),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeDocument>>) rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(),
                actor.actorId(), documentId, actor.tenantId(), workspaceId, assetVersion, actor.actorId()).orElseThrow(EafException::notFound);
    }

    @Override
    @Transactional
    /** 只对授权的草稿版本执行确定性切块；重复调用复用数据库中同版本结果。 */
    public List<KnowledgeChunk> chunk(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                     String chunkingVersion) {
        if (actor == null || workspaceId == null || documentId == null)
            throw EafException.invalid("切块上下文不能为空。");
        if (assetVersion < 1) throw EafException.invalid("assetVersion 必须为正数。");
        var maxCodePoints = CHUNK_LIMITS.get(chunkingVersion);
        if (maxCodePoints == null) throw EafException.invalid("不支持的 chunkingVersion。");
        var access = workspaces.require(actor, workspaceId, "knowledge:write");
        var source = jdbc.query("select d.source_ref, v.asset_version, v.content from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? and v.status in ('DRAFT', 'PUBLISHED') where d.id = ? and d.tenant_id = ? and d.workspace_id = ? and d.status in ('DRAFT', 'PUBLISHED') and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:write' and p.status = 'ACTIVE') and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE')",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ChunkSource>>) rs -> rs.next() ? Optional.of(new ChunkSource(rs.getString("source_ref"), rs.getInt("asset_version"), rs.getString("content"))) : Optional.empty(),
                assetVersion, documentId, access.tenantId(), access.workspaceId(), actor.actorId(), actor.actorId()).orElseThrow(EafException::notFound);
        requireCurrentManagedSourceVersion(actor, access, documentId, assetVersion);
        var ranges = "p9-structure-1".equals(chunkingVersion)
                ? splitStructured(source.content(), maxCodePoints) : split(source.content(), maxCodePoints);
        var now = Timestamp.from(Instant.now(clock));
        for (var range : ranges) {
            var chunk = source.content().substring(range.startChar(), range.endChar());
            jdbc.update("insert into knowledge.chunk(id, tenant_id, workspace_id, document_id, asset_version, source_ref, chunking_version, chunk_order, start_offset, end_offset, offset_unit, content, content_hash, heading_path, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNICODE_CODE_POINT', ?, ?, ?::jsonb, ?) on conflict (tenant_id, workspace_id, document_id, asset_version, chunking_version, chunk_order) do nothing",
                    UUID.randomUUID(), access.tenantId(), access.workspaceId(), documentId, source.assetVersion(), source.sourceRef(), chunkingVersion,
                    range.order(), range.startCodePoint(), range.endCodePoint(), chunk, Hashing.sha256(chunk), jsonString(range.headingPath()), now);
        }
        return jdbc.query("select id, tenant_id, workspace_id, document_id, asset_version, source_ref, chunking_version, chunk_order, start_offset, end_offset, offset_unit, content, content_hash, heading_path::text from knowledge.chunk where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ? and chunking_version = ? order by chunk_order",
                (rs, rowNum) -> new KnowledgeChunk(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("document_id", UUID.class), rs.getInt("asset_version"),
                        rs.getString("source_ref"), rs.getString("chunking_version"), rs.getInt("chunk_order"),
                        rs.getInt("start_offset"), rs.getInt("end_offset"), rs.getString("offset_unit"),
                        rs.getString("content"), rs.getString("content_hash"), stringList(rs.getString("heading_path"))),
                access.tenantId(), access.workspaceId(), documentId, source.assetVersion(), chunkingVersion);
    }

    @Override
    public KnowledgeIndexBuild buildIndex(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                          String chunkingVersion, String idempotencyKey) {
        if (blank(idempotencyKey) || idempotencyKey.length() > 200 || hasControl(idempotencyKey))
            throw EafException.invalid("索引构建幂等键必须为 1-200 个可记录字符。");
        if (!CHUNK_LIMITS.containsKey(chunkingVersion)) throw EafException.invalid("不支持的 chunkingVersion。");
        if (assetVersion < 1) throw EafException.invalid("assetVersion 必须为正数。");
        var access = workspaces.require(actor, workspaceId, "knowledge:write");
        var source = findBuildSource(actor, access, documentId, assetVersion, chunkingVersion).orElseThrow(EafException::notFound);
        requireCurrentManagedSourceVersion(actor, access, documentId, assetVersion);
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        requireExternalEmbedding(actor, workspaceId, profile);
        if (source.totalChunks() == 0) throw EafException.conflict("INDEX_BUILD_NO_CHUNKS", "文档尚未完成切块，不能构建索引。");
        var signature = configurationSignature(chunkingVersion, profile);
        var now = Instant.now(clock);
        var buildId = UUID.randomUUID();
        var lexicalIndexVersion = "p9-structure-1".equals(chunkingVersion) ? "p9-lexical-1" : "none";
        int inserted;
        try {
            inserted = jdbc.update("insert into knowledge.index_build(id, tenant_id, workspace_id, document_id, asset_version, chunking_version, provider, model, model_revision, dimension, distance_metric, configuration_signature, idempotency_key, max_batch_size, max_input_tokens, max_text_code_points, lexical_index_version, status, total_chunks, completed_chunks, next_chunk_order, input_tokens, embedding_calls, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COSINE', ?, ?, ?, ?, ?, ?, 'PENDING', ?, 0, 1, 0, 0, ?, ?) on conflict do nothing",
                    buildId, access.tenantId(), access.workspaceId(), documentId, source.assetVersion(), chunkingVersion,
                    profile.provider(), profile.model(), profile.revision(), profile.dimension(), signature,
                    idempotencyKey, profile.maxBatchSize(), profile.maxInputTokens(), profile.maxTextCodePoints(), lexicalIndexVersion,
                    source.totalChunks(), Timestamp.from(now), Timestamp.from(now));
        } catch (DuplicateKeyException e) {
            // 不同数据库驱动对多重唯一约束的冲突返回码不完全一致；冲突后重新读取事实记录即可收敛。
            return getBuildForWrite(actor, access, documentId, assetVersion, idempotencyKey, signature);
        }
        if (inserted == 0) return getBuildForWrite(actor, access, documentId, assetVersion, idempotencyKey, signature);
        // 先短事务领取作业，再在事务外调用模型；重复请求不会重复领取或伪造第二个 build。
        if (jdbc.update("update knowledge.index_build set status = 'INDEXING', started_at = ?, updated_at = ? where id = ? and status = 'PENDING'",
                Timestamp.from(now), Timestamp.from(now), buildId) == 1) processBuild(buildId, actor, access, documentId);
        return getIndexBuild(actor, workspaceId, documentId, buildId);
    }

    @Override
    public KnowledgeIndexBuild getIndexBuild(ActorContext actor, UUID workspaceId, UUID documentId, UUID buildId) {
        var access = isManagedSourceOwner(actor, workspaceId, documentId)
                ? requireSourceManager(actor, workspaceId) : workspaces.require(actor, workspaceId, "knowledge:read");
        return jdbc.query("select b.* from knowledge.index_build b where b.id = ? and b.tenant_id = ? and b.workspace_id = ? and b.document_id = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id and p.document_id = b.document_id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE')",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeIndexBuild>>) rs -> rs.next() ? Optional.of(mapBuild(rs)) : Optional.empty(),
                buildId, access.tenantId(), access.workspaceId(), documentId, actor.actorId()).orElseThrow(EafException::notFound);
    }

    @Override
    public KnowledgeIndexBuild retryIndex(ActorContext actor, UUID workspaceId, UUID documentId, UUID buildId) {
        var access = workspaces.require(actor, workspaceId, "knowledge:write");
        var build = getIndexBuild(actor, workspaceId, documentId, buildId);
        requireCurrentManagedSourceVersion(actor, access, documentId, build.assetVersion());
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        requireExternalEmbedding(actor, workspaceId, profile);
        if ("READY".equals(build.status())) return build;
        var now = Instant.now(clock);
        // FAILED 可以立即重试；疑似进程中断的 INDEXING 仅在调用截止时间后原子接管，防止并发重复付费。
        if (jdbc.update("update knowledge.index_build set status = 'INDEXING', failure_code = null, retry_generation = retry_generation + 1, started_at = coalesce(started_at, ?), updated_at = ? where id = ? and (status in ('FAILED', 'PENDING') or (status = 'INDEXING' and updated_at < ?))",
                Timestamp.from(now), Timestamp.from(now), buildId, Timestamp.from(now.minusSeconds(60))) == 1)
            processBuild(buildId, actor, access, documentId);
        return getIndexBuild(actor, workspaceId, documentId, buildId);
    }

    @Override
    @Transactional
    public KnowledgePublication publish(ActorContext actor, UUID workspaceId, UUID documentId,
                                        long expectedRowVersion, UUID buildId, String idempotencyKey) {
        return publish(actor, workspaceId, documentId, expectedRowVersion, null, buildId, idempotencyKey);
    }

    @Override
    @Transactional
    public KnowledgePublication publish(ActorContext actor, UUID workspaceId, UUID documentId, long expectedRowVersion,
                                        Integer expectedBaseVersion, UUID buildId, String idempotencyKey) {
        return publishInternal(actor, workspaceId, documentId, expectedRowVersion, expectedBaseVersion,
                buildId, idempotencyKey, null, null, null);
    }

    @Override
    @Transactional
    public KnowledgePublication publishCandidate(ActorContext actor, UUID workspaceId, UUID documentId,
                                                  long expectedRowVersion, int expectedBaseVersion, UUID buildId,
                                                  UUID candidateId, int candidateRevision, String contentHash) {
        if (candidateId == null || candidateRevision < 1 || contentHash == null || !contentHash.matches("[0-9a-f]{64}"))
            throw EafException.invalid("候选 Knowledge 发布来源或正文摘要无效。");
        return publishInternal(actor, workspaceId, documentId, expectedRowVersion, expectedBaseVersion, buildId,
                "learning-candidate:" + candidateId + ":" + candidateRevision,
                candidateId, candidateRevision, contentHash);
    }

    private KnowledgePublication publishInternal(ActorContext actor, UUID workspaceId, UUID documentId,
                                                  long expectedRowVersion, Integer expectedBaseVersion, UUID buildId,
                                                  String idempotencyKey, UUID candidateId,
                                                  Integer candidateRevision, String expectedContentHash) {
        validatePublicationCommand(expectedRowVersion, buildId, idempotencyKey);
        if (expectedBaseVersion != null && expectedBaseVersion < 0) throw EafException.invalid("baseVersion 不能小于零。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        var requestHash = candidateId == null
                ? Hashing.sha256("PUBLISH|" + expectedRowVersion + "|" + expectedBaseVersion + "|" + buildId)
                : Hashing.sha256("PUBLISH|" + expectedRowVersion + "|" + expectedBaseVersion + "|" + buildId
                    + "|" + candidateId + "|" + candidateRevision + "|" + expectedContentHash);
        var prior = findPublicationEvent(access, documentId, idempotencyKey);
        if (prior.isPresent()) return sameCommand(prior.get(), requestHash);
        var document = lockDocumentForPublication(access, actor, documentId).orElseThrow(EafException::notFound);
        var concurrent = findPublicationEvent(access, documentId, idempotencyKey);
        if (concurrent.isPresent()) return sameCommand(concurrent.get(), requestHash);
        if (document.rowVersion() != expectedRowVersion)
            throw EafException.conflict("VERSION_CONFLICT", "文档版本已变化，请重新读取后发布。");
        if (!List.of("DRAFT", "PUBLISHED").contains(document.status()))
            throw EafException.conflict("PUBLICATION_STATE_CONFLICT", "只有草稿或已发布文档可以切换索引。");
        var currentAssetVersion = currentVersion(access, documentId);
        if (expectedBaseVersion != null && (currentAssetVersion == null ? 0 : currentAssetVersion) != expectedBaseVersion)
            throw EafException.conflict("BASE_VERSION_CONFLICT", "当前发布版本与 baseVersion 不一致。");
        var build = jdbc.query("select b.id, b.asset_version from knowledge.index_build b where b.id = ? and b.tenant_id = ? and b.workspace_id = ? and b.document_id = ? and b.status = 'READY' and b.total_chunks = b.completed_chunks and b.completed_chunks = (select count(*) from knowledge.embedding e where e.build_id = b.id) and (b.lexical_index_version = 'none' or b.total_chunks = (select count(*) from knowledge.lexical_entry le where le.build_id = b.id))",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<ReadyBuild>>) rs -> rs.next() ? Optional.of(new ReadyBuild(rs.getObject("id", UUID.class), rs.getInt("asset_version"))) : Optional.empty(),
                buildId, access.tenantId(), access.workspaceId(), documentId).orElseThrow(() -> EafException.conflict("PUBLICATION_NOT_READY", "目标索引尚未 READY。"));
        var version = jdbc.query("select content_hash, status from knowledge.document_version where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<VersionState>>) rs -> rs.next()
                        ? Optional.of(new VersionState(rs.getString("content_hash"), rs.getString("status"))) : Optional.empty(),
                access.tenantId(), workspaceId, documentId, build.assetVersion()).orElseThrow(() -> EafException.conflict("VERSION_NOT_FOUND", "目标知识版本不存在。"));
        if (expectedContentHash != null && !expectedContentHash.equals(version.contentHash()))
            throw EafException.conflict("CONTENT_HASH_CONFLICT", "候选正文摘要与待发布 Knowledge 版本不一致。");
        if (isManagedSourceDocument(access, documentId)) {
            if (candidateId != null)
                throw EafException.conflict("MANAGED_SOURCE_CANDIDATE_CONFLICT", "受管来源文档不能通过 Learning 候选发布改写。");
            requireCurrentManagedSourceVersion(actor, access, documentId, build.assetVersion());
        }
        if (!List.of("DRAFT", "PUBLISHED").contains(version.status()))
            throw EafException.conflict("VERSION_STATE_CONFLICT", "已撤回的知识版本不能重新发布。");
        var now = Timestamp.from(Instant.now(clock));
        var nextVersion = expectedRowVersion + 1;
        jdbc.update("update knowledge.document set status = 'PUBLISHED', row_version = ?, updated_at = ? where tenant_id = ? and workspace_id = ? and id = ? and row_version = ?",
                nextVersion, now, access.tenantId(), access.workspaceId(), documentId, expectedRowVersion);
        if ("DRAFT".equals(version.status()))
            jdbc.update("update knowledge.document_version set status = 'PUBLISHED' where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ? and status = 'DRAFT'",
                    access.tenantId(), access.workspaceId(), documentId, build.assetVersion());
        // 切换当前指针只影响新检索；旧发布版本与 READY build 留作历史任务核对。
        jdbc.update("insert into knowledge.document_publication(tenant_id, workspace_id, document_id, asset_version, build_id, status, row_version, updated_at) values (?, ?, ?, ?, ?, 'ACTIVE', 1, ?) on conflict (tenant_id, workspace_id, document_id) do update set asset_version = excluded.asset_version, build_id = excluded.build_id, status = 'ACTIVE', row_version = knowledge.document_publication.row_version + 1, updated_at = excluded.updated_at",
                access.tenantId(), access.workspaceId(), documentId, build.assetVersion(), build.id(), now);
        var eventId = UUID.randomUUID();
        jdbc.update("insert into knowledge.publication_event(id, tenant_id, workspace_id, document_id, asset_version, build_id, action, document_status, document_row_version, idempotency_key, request_hash, occurred_at, release_origin, candidate_id, candidate_revision, base_version, content_hash) values (?, ?, ?, ?, ?, ?, 'PUBLISHED', 'PUBLISHED', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                eventId, access.tenantId(), access.workspaceId(), documentId, build.assetVersion(), build.id(), nextVersion,
                idempotencyKey, requestHash, now, candidateId == null ? "OWNER" : "LEARNING_CANDIDATE",
                candidateId, candidateRevision, expectedBaseVersion, version.contentHash());
        var publication = new KnowledgePublication(eventId, access.tenantId(), access.workspaceId(), documentId,
                build.assetVersion(), build.id(), "PUBLISHED", "PUBLISHED", nextVersion, now.toInstant(),
                candidateId == null ? "OWNER" : "LEARNING_CANDIDATE", candidateId, candidateRevision,
                expectedBaseVersion, version.contentHash());
        appendPublicationOutbox(publication);
        return publication;
    }

    @Override
    @Transactional
    public KnowledgePublication revoke(ActorContext actor, UUID workspaceId, UUID documentId, long expectedRowVersion,
                                       String idempotencyKey) {
        if (expectedRowVersion <= 0 || blank(idempotencyKey) || idempotencyKey.length() > 200 || hasControl(idempotencyKey))
            throw EafException.invalid("撤回命令需要有效 expectedRowVersion 和幂等键。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        var requestHash = Hashing.sha256("REVOKE_CURRENT|" + expectedRowVersion);
        var prior = findPublicationEvent(access, documentId, idempotencyKey);
        if (prior.isPresent()) return sameCommand(prior.get(), requestHash);
        var pointer = jdbc.query("select asset_version from knowledge.document_publication where tenant_id = ? and workspace_id = ? and document_id = ? and status = 'ACTIVE'",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<Integer>>) rs -> rs.next() ? Optional.of(rs.getInt("asset_version")) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId).orElseThrow(() -> EafException.conflict("PUBLICATION_STATE_CONFLICT", "当前发布指针不存在。"));
        return revokeVersion(actor, workspaceId, documentId, pointer, expectedRowVersion, idempotencyKey,
                requestHash, true, null, null, null);
    }

    @Override
    @Transactional
    public KnowledgePublication revokeVersion(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                              long expectedRowVersion, String idempotencyKey) {
        if (assetVersion < 1 || expectedRowVersion <= 0 || blank(idempotencyKey) || idempotencyKey.length() > 200 || hasControl(idempotencyKey))
            throw EafException.invalid("版本撤回需要 assetVersion、expectedRowVersion 和有效幂等键。");
        return revokeVersion(actor, workspaceId, documentId, assetVersion, expectedRowVersion, idempotencyKey,
                Hashing.sha256("REVOKE|" + expectedRowVersion + "|" + assetVersion), false, null, null, null);
    }

    @Override
    @Transactional
    public KnowledgePublication revokeCandidateVersion(ActorContext actor, UUID workspaceId, UUID documentId,
                                                        int assetVersion, long expectedRowVersion,
                                                        UUID candidateId, int candidateRevision) {
        if (assetVersion < 1 || expectedRowVersion <= 0 || candidateId == null || candidateRevision < 1)
            throw EafException.invalid("学习撤回需要具体版本和有效来源键。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        var key = "learning-withdrawal:" + candidateId + ":" + candidateRevision;
        var requestHash = Hashing.sha256("LEARNING_REVOKE|" + candidateId + "|" + candidateRevision
                + "|" + documentId + "|" + assetVersion + "|" + expectedRowVersion);
        var prior = findPublicationEvent(access, documentId, key);
        if (prior.isPresent()) return sameCommand(prior.get(), requestHash);
        var originBaseVersion = jdbc.query("select base_version from knowledge.publication_event where tenant_id = ? "
                        + "and workspace_id = ? and document_id = ? and asset_version = ? and action = 'PUBLISHED' "
                        + "and release_origin = 'LEARNING_CANDIDATE' and candidate_id = ? and candidate_revision = ? "
                        + "order by occurred_at desc, id desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<Integer>>) rs -> rs.next()
                        ? Optional.ofNullable((Integer) rs.getObject("base_version")) : Optional.empty(),
                access.tenantId(), workspaceId, documentId, assetVersion, candidateId, candidateRevision)
                .orElseThrow(() -> EafException.conflict("RELEASE_NOT_FOUND", "该 Knowledge 版本不属于指定候选修订。"));
        return revokeVersion(actor, workspaceId, documentId, assetVersion, expectedRowVersion, key, requestHash,
                false, candidateId, candidateRevision, originBaseVersion);
    }

    private KnowledgePublication revokeVersion(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                                long expectedRowVersion, String idempotencyKey, String requestHash,
                                                boolean legacyDocumentRevoke, UUID candidateId,
                                                Integer candidateRevision, Integer baseVersion) {
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        var prior = findPublicationEvent(access, documentId, idempotencyKey);
        if (prior.isPresent()) return sameCommand(prior.get(), requestHash);
        var document = lockDocumentForPublication(access, actor, documentId).orElseThrow(EafException::notFound);
        var concurrent = findPublicationEvent(access, documentId, idempotencyKey);
        if (concurrent.isPresent()) return sameCommand(concurrent.get(), requestHash);
        if (document.rowVersion() != expectedRowVersion)
            throw EafException.conflict("VERSION_CONFLICT", "文档版本已变化，请重新读取后撤回。");
        var version = jdbc.query("select content_hash, status from knowledge.document_version where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<VersionState>>) rs -> rs.next()
                        ? Optional.of(new VersionState(rs.getString("content_hash"), rs.getString("status"))) : Optional.empty(),
                access.tenantId(), workspaceId, documentId, assetVersion).orElseThrow(EafException::notFound);
        if (!"PUBLISHED".equals(version.status()))
            throw EafException.conflict("VERSION_STATE_CONFLICT", "只有已发布版本可以撤回。");
        var build = jdbc.query("select build_id from knowledge.publication_event where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ? and action = 'PUBLISHED' order by occurred_at desc, id desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<UUID>>) rs -> rs.next() ? Optional.of(rs.getObject("build_id", UUID.class)) : Optional.empty(),
                access.tenantId(), workspaceId, documentId, assetVersion).orElseThrow(() -> EafException.conflict("PUBLICATION_STATE_CONFLICT", "该版本没有 READY 索引可供审计。"));
        var current = jdbc.query("select asset_version from knowledge.document_publication where tenant_id = ? and workspace_id = ? and document_id = ? and status = 'ACTIVE'",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<Integer>>) rs -> rs.next() ? Optional.of(rs.getInt("asset_version")) : Optional.empty(),
                access.tenantId(), workspaceId, documentId);
        var isCurrent = current.isPresent() && current.get() == assetVersion;
        var fallbackBuild = isCurrent && !legacyDocumentRevoke && candidateId != null
                && baseVersion != null && baseVersion != assetVersion
                ? jdbc.query("select e.build_id from knowledge.publication_event e "
                                + "join knowledge.document_version v on v.tenant_id = e.tenant_id "
                                + "and v.workspace_id = e.workspace_id and v.document_id = e.document_id "
                                + "and v.asset_version = e.asset_version "
                                + "join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id "
                                + "and b.workspace_id = e.workspace_id and b.document_id = e.document_id "
                                + "and b.asset_version = e.asset_version and b.status = 'READY' "
                                + "where e.tenant_id = ? and e.workspace_id = ? and e.document_id = ? "
                                + "and e.asset_version = ? and e.action = 'PUBLISHED' and v.status = 'PUBLISHED' "
                                + "order by e.occurred_at desc, e.id desc limit 1",
                        (org.springframework.jdbc.core.ResultSetExtractor<Optional<UUID>>) rs -> rs.next()
                                ? Optional.of(rs.getObject("build_id", UUID.class)) : Optional.empty(),
                        access.tenantId(), workspaceId, documentId, baseVersion)
                : Optional.empty();
        var now = Timestamp.from(Instant.now(clock));
        var nextVersion = expectedRowVersion + 1;
        // Learning 撤回优先回切仍发布且索引 READY 的精确基线；没有合法基线时保持无当前发布。
        var restoreBase = isCurrent && fallbackBuild.isPresent();
        var nextDocumentStatus = isCurrent
                ? legacyDocumentRevoke ? "REVOKED" : restoreBase ? "PUBLISHED" : "DRAFT"
                : document.status();
        jdbc.update("update knowledge.document set status = ?, row_version = ?, updated_at = ? where tenant_id = ? and workspace_id = ? and id = ? and row_version = ?",
                nextDocumentStatus, nextVersion, now, access.tenantId(), workspaceId, documentId, expectedRowVersion);
        jdbc.update("update knowledge.document_version set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ? and status = 'PUBLISHED'",
                access.tenantId(), access.workspaceId(), documentId, assetVersion);
        if (isCurrent) jdbc.update("update knowledge.document_publication set status = 'REVOKED', row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? and document_id = ?",
                now, access.tenantId(), access.workspaceId(), documentId);
        var eventId = UUID.randomUUID();
        // 撤回当前学习版本后，仅回切到发布事实仍有效且索引 READY 的精确基线。
        if (restoreBase) jdbc.update("update knowledge.document_publication set asset_version = ?, build_id = ?, status = 'ACTIVE', updated_at = ? where tenant_id = ? and workspace_id = ? and document_id = ?",
                baseVersion, fallbackBuild.orElseThrow(), now, access.tenantId(), workspaceId, documentId);
        var releaseOrigin = candidateId == null ? "OWNER" : "LEARNING_CANDIDATE";
        jdbc.update("insert into knowledge.publication_event(id, tenant_id, workspace_id, document_id, asset_version, build_id, action, document_status, document_row_version, idempotency_key, request_hash, occurred_at, release_origin, candidate_id, candidate_revision, base_version, content_hash) values (?, ?, ?, ?, ?, ?, 'REVOKED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                eventId, access.tenantId(), access.workspaceId(), documentId, assetVersion, build, nextDocumentStatus,
                nextVersion, idempotencyKey, requestHash, now, releaseOrigin, candidateId, candidateRevision,
                baseVersion, version.contentHash());
        var publication = new KnowledgePublication(eventId, access.tenantId(), access.workspaceId(), documentId, assetVersion,
                build, "REVOKED", nextDocumentStatus, nextVersion, now.toInstant(), releaseOrigin, candidateId,
                candidateRevision, baseVersion, version.contentHash());
        appendPublicationOutbox(publication);
        return publication;
    }

    @Override
    public KnowledgePublication getCurrentPublication(ActorContext actor, UUID workspaceId, UUID documentId) {
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        return jdbc.query("select e.id, e.tenant_id, e.workspace_id, e.document_id, e.asset_version, e.build_id, e.action, e.document_status, e.document_row_version, e.occurred_at, e.release_origin, e.candidate_id, e.candidate_revision, e.base_version, e.content_hash from knowledge.publication_event e join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and e.asset_version = p.asset_version and e.build_id = p.build_id and p.status = 'ACTIVE' where e.tenant_id = ? and e.workspace_id = ? and e.document_id = ? and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = e.tenant_id and dp.workspace_id = e.workspace_id and dp.document_id = e.document_id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') order by e.occurred_at desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgePublication>>) rs -> rs.next() ? Optional.of(mapPublicationEvent(rs)) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, actor.actorId()).orElseThrow(EafException::notFound);
    }

    // Learning 只能查询带来源键的发布事实，权限仍按目标 Knowledge 文档逐条过滤。
    @Override
    public KnowledgePublication findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                     int candidateRevision) {
        if (candidateId == null || candidateRevision < 1) throw EafException.invalid("来源键无效。");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        return jdbc.query("select e.id, e.tenant_id, e.workspace_id, e.document_id, e.asset_version, e.build_id, e.action, e.document_status, e.document_row_version, e.occurred_at, e.release_origin, e.candidate_id, e.candidate_revision, e.base_version, e.content_hash from knowledge.publication_event e where e.tenant_id = ? and e.workspace_id = ? and e.release_origin = 'LEARNING_CANDIDATE' and e.candidate_id = ? and e.candidate_revision = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE') order by e.occurred_at desc, e.id desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgePublication>>) rs -> rs.next() ? Optional.of(mapPublicationEvent(rs)) : Optional.empty(),
                access.tenantId(), access.workspaceId(), candidateId, candidateRevision, actor.actorId())
                .orElseThrow(EafException::notFound);
    }

    @Override
    public KnowledgePublication findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                        int candidateRevision) {
        if (candidateId == null || candidateRevision < 1) throw EafException.invalid("来源键无效。");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        return jdbc.query("select e.id, e.tenant_id, e.workspace_id, e.document_id, e.asset_version, e.build_id, "
                        + "e.action, e.document_status, e.document_row_version, e.occurred_at, e.release_origin, "
                        + "e.candidate_id, e.candidate_revision, e.base_version, e.content_hash "
                        + "from knowledge.publication_event e where e.tenant_id = ? and e.workspace_id = ? "
                        + "and e.action = 'REVOKED' and e.release_origin = 'LEARNING_CANDIDATE' "
                        + "and e.candidate_id = ? and e.candidate_revision = ? and exists "
                        + "(select 1 from knowledge.document_permission p where p.tenant_id = e.tenant_id "
                        + "and p.workspace_id = e.workspace_id and p.document_id = e.document_id "
                        + "and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE') "
                        + "order by e.occurred_at desc, e.id desc limit 1",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgePublication>>) rs -> rs.next()
                        ? Optional.of(mapPublicationEvent(rs)) : Optional.empty(),
                access.tenantId(), workspaceId, candidateId, candidateRevision, actor.actorId())
                .orElseThrow(EafException::notFound);
    }

    @Override
    public boolean isUsable(ActorContext actor, UUID workspaceId, UUID documentId, int documentVersion,
                            UUID chunkId, UUID buildId, String contentHash) {
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        var delegatedScope = mcpDocumentScope(actor, workspaceId);
        if (delegatedScope != null && (!delegatedScope.knowledgeDocumentIds().contains(documentId)
                || !hasDocumentReadPermission(actor.principalId(), access.tenantId(), workspaceId, documentId))) return false;
        var sql = "select count(*) from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? and v.status = 'PUBLISHED' join knowledge.chunk c on c.tenant_id = v.tenant_id and c.workspace_id = v.workspace_id and c.document_id = v.document_id and c.asset_version = v.asset_version and c.id = ? and c.content_hash = ? join knowledge.index_build b on b.id = ? and b.tenant_id = d.tenant_id and b.workspace_id = d.workspace_id and b.document_id = d.id and b.asset_version = v.asset_version and b.status = 'READY' where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and d.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')"
                + managedSourceReadGuard();
        var parameters = new ArrayList<Object>();
        parameters.add(documentVersion);
        parameters.add(chunkId);
        parameters.add(contentHash);
        parameters.add(buildId);
        parameters.add(access.tenantId());
        parameters.add(access.workspaceId());
        parameters.add(documentId);
        parameters.add(actor.actorId());
        parameters.add(actor.actorId());
        if (delegatedScope != null) {
            sql += managedSourceReadGuard();
            parameters.add(delegatedScope.ownerId());
        }
        var count = jdbc.queryForObject(sql, Integer.class, parameters.toArray());
        return count != null && count == 1;
    }

    @Override
    public List<PublishedKnowledgeChunk> readPublishedChunks(ActorContext actor, UUID workspaceId,
            List<PublishedKnowledgeChunk.Ref> refs) {
        requireDirectHuman(actor, "已选 Knowledge Chunk 读取");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        if (!identities.isActiveHuman(actor.tenantId(), actor.actorId())) throw EafException.notFound();
        if (refs == null || refs.isEmpty() || refs.size() > 10 || refs.stream().anyMatch(ref -> ref == null
                || ref.documentId() == null || ref.documentVersion() < 1 || ref.chunkId() == null || ref.buildId() == null
                || ref.contentHash() == null || !ref.contentHash().matches("[0-9a-f]{64}"))
                || refs.stream().map(PublishedKnowledgeChunk.Ref::chunkId).distinct().count() != refs.size())
            throw EafException.invalid("精确 Knowledge 引用必须为 1-10 个且不得重复。");
        return refs.stream().map(ref -> jdbc.query(
                "select c.id, c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.source_ref, "
                        + "c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, "
                        + "c.content, c.content_hash, c.heading_path::text, d.title "
                        + "from knowledge.chunk c join knowledge.document d on d.tenant_id = c.tenant_id "
                        + "and d.workspace_id = c.workspace_id and d.id = c.document_id "
                        + "join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "and v.document_id = d.id and v.asset_version = c.asset_version "
                        + "join knowledge.index_build b on b.id = ? and b.tenant_id = c.tenant_id "
                        + "and b.workspace_id = c.workspace_id and b.document_id = c.document_id "
                        + "and b.asset_version = c.asset_version and b.chunking_version = c.chunking_version and b.status = 'READY' "
                        + "join knowledge.document_publication p on p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id "
                        + "and p.document_id = b.document_id and p.asset_version = b.asset_version and p.build_id = b.id and p.status = 'ACTIVE' "
                        + "where c.id = ? and c.document_id = ? and c.asset_version = ? and c.content_hash = ? "
                        + "and d.tenant_id = ? and d.workspace_id = ? and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' "
                        + "and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id "
                        + "and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? "
                        + "and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')" + managedSourceReadGuard(),
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<PublishedKnowledgeChunk>>) rs -> rs.next()
                        ? Optional.of(new PublishedKnowledgeChunk(ref, rs.getString("title"), rs.getString("content"),
                        stringList(rs.getString("heading_path")))) : Optional.empty(),
                ref.buildId(), ref.chunkId(), ref.documentId(), ref.documentVersion(), ref.contentHash(),
                access.tenantId(), workspaceId, actor.actorId(), actor.actorId())
                .orElseThrow(EafException::notFound)).toList();
    }

    @Override
    @Transactional
    public ContextShare shareContext(ActorContext actor, UUID workspaceId, UUID documentId, UUID recipientId,
                                    int documentVersion, long expectedVersion) {
        requireHumanContextShareActor(actor, workspaceId);
        if (documentId == null || recipientId == null || recipientId.equals(actor.actorId())
                || documentVersion < 1 || expectedVersion < 0)
            throw EafException.invalid("正式知识共享参数无效。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        workspaces.require(actor, workspaceId, "knowledge:read");
        var profile = workspaceCatalog.profile(access.tenantId(), workspaceId);
        if (profile.kind() == WorkspaceKind.PERSONAL) throw EafException.forbidden("个人空间资料不能通过正式知识共享入口转出。");
        if (!identities.isActiveHuman(access.tenantId(), recipientId)
                || !workspaces.isAuthorized(access.tenantId(), recipientId, workspaceId, "context:read")
                || !workspaces.isAuthorized(access.tenantId(), recipientId, workspaceId, "knowledge:read"))
            throw EafException.notFound();
        var doc = jdbc.query("select owner_id from knowledge.document where tenant_id = ? and workspace_id = ? and id = ? and status = 'PUBLISHED' for update",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, access.tenantId(), workspaceId, documentId);
        if (doc == null || !doc.equals(actor.actorId())) throw EafException.notFound();
        var current = jdbc.query("select p.asset_version, p.build_id, v.content_hash from knowledge.document_publication p "
                        + "join knowledge.document_version v on v.tenant_id = p.tenant_id and v.workspace_id = p.workspace_id "
                        + "and v.document_id = p.document_id and v.asset_version = p.asset_version "
                        + "join knowledge.index_build b on b.id = p.build_id and b.tenant_id = p.tenant_id and b.workspace_id = p.workspace_id "
                        + "and b.document_id = p.document_id and b.asset_version = p.asset_version and b.status = 'READY' "
                        + "and b.total_chunks = b.completed_chunks and b.completed_chunks = (select count(*) from knowledge.embedding e where e.build_id = b.id) "
                        + "where p.tenant_id = ? and p.workspace_id = ? and p.document_id = ? and p.status = 'ACTIVE' and v.status = 'PUBLISHED'",
                rs -> rs.next() ? new PublicationBinding(rs.getInt("asset_version"), rs.getObject("build_id", UUID.class), rs.getString("content_hash")) : null,
                access.tenantId(), workspaceId, documentId);
        if (current == null || current.assetVersion() != documentVersion)
            throw EafException.conflict("PUBLICATION_NOT_CURRENT", "共享只能绑定当前正式发布且索引就绪的版本。");
        var row = jdbc.query("select id, recipient_id, document_version, build_id, content_hash, status, row_version, created_by, created_at, updated_at "
                        + "from knowledge.context_share where tenant_id = ? and workspace_id = ? and document_id = ? and recipient_id = ? for update",
                rs -> rs.next() ? mapContextShare(rs, access.tenantId(), workspaceId, documentId) : null,
                access.tenantId(), workspaceId, documentId, recipientId);
        var now = Timestamp.from(Instant.now(clock));
        if (row == null) {
            if (expectedVersion != 0) throw EafException.conflict("VERSION_CONFLICT", "首次共享的 expectedVersion 必须为 0。");
            var id = UUID.randomUUID();
            jdbc.update("insert into knowledge.context_share(id, tenant_id, workspace_id, document_id, recipient_id, document_version, build_id, content_hash, status, row_version, created_by, created_at, updated_at) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', 1, ?, ?, ?)", id, access.tenantId(), workspaceId,
                    documentId, recipientId, documentVersion, current.buildId(), current.contentHash(), actor.actorId(), now, now);
            appendContextShareAudit(actor, workspaceId, documentId, recipientId, "knowledge.context-share.created");
            return contextShare(access.tenantId(), workspaceId, documentId, recipientId);
        }
        if ("ACTIVE".equals(row.status()) && row.documentVersion() == documentVersion
                && row.buildId().equals(current.buildId()) && row.contentHash().equals(current.contentHash())) return row;
        if (row.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "正式知识共享版本已变化，请先读取当前共享状态。");
        jdbc.update("update knowledge.context_share set document_version = ?, build_id = ?, content_hash = ?, status = 'ACTIVE', row_version = row_version + 1, updated_at = ? "
                        + "where id = ? and row_version = ?", documentVersion, current.buildId(), current.contentHash(), now,
                row.id(), expectedVersion);
        appendContextShareAudit(actor, workspaceId, documentId, recipientId, "knowledge.context-share.updated");
        return contextShare(access.tenantId(), workspaceId, documentId, recipientId);
    }

    @Override
    public ContextSharePage listContextShares(ActorContext actor, UUID workspaceId, UUID documentId, int limit, int offset) {
        requireHumanContextShareActor(actor, workspaceId);
        if (limit < 1 || limit > 50 || offset < 0) throw EafException.invalid("共享列表 limit 或 offset 无效。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        requireDocumentOwner(access.tenantId(), workspaceId, documentId, actor.actorId());
        var shares = jdbc.query("select id, recipient_id, document_version, build_id, content_hash, status, row_version, created_by, created_at, updated_at "
                        + "from knowledge.context_share where tenant_id = ? and workspace_id = ? and document_id = ? order by created_at, id limit ? offset ?",
                (rs, row) -> mapContextShare(rs, access.tenantId(), workspaceId, documentId),
                access.tenantId(), workspaceId, documentId, limit + 1, offset);
        var hasNext = shares.size() > limit;
        var items = shares.stream().limit(limit).map(share -> withValidity(share, access.tenantId())).toList();
        return new ContextSharePage(items, hasNext ? offset + limit : null);
    }

    @Override
    @Transactional
    public ContextShare revokeContextShare(ActorContext actor, UUID workspaceId, UUID documentId, UUID recipientId,
                                          long expectedVersion) {
        requireHumanContextShareActor(actor, workspaceId);
        if (recipientId == null || expectedVersion < 0) throw EafException.invalid("共享撤回参数无效。");
        var access = workspaces.require(actor, workspaceId, "knowledge:publish");
        requireDocumentOwner(access.tenantId(), workspaceId, documentId, actor.actorId());
        var row = jdbc.query("select id, recipient_id, document_version, build_id, content_hash, status, row_version, created_by, created_at, updated_at "
                        + "from knowledge.context_share where tenant_id = ? and workspace_id = ? and document_id = ? and recipient_id = ? for update",
                rs -> rs.next() ? mapContextShare(rs, access.tenantId(), workspaceId, documentId) : null,
                access.tenantId(), workspaceId, documentId, recipientId);
        if (row == null) throw EafException.notFound();
        if ("REVOKED".equals(row.status())) return row;
        if (row.rowVersion() != expectedVersion)
            throw EafException.conflict("VERSION_CONFLICT", "正式知识共享版本已变化，请先读取当前共享状态。");
        jdbc.update("update knowledge.context_share set status = 'REVOKED', row_version = row_version + 1, updated_at = ? where id = ? and row_version = ?",
                Timestamp.from(Instant.now(clock)), row.id(), expectedVersion);
        appendContextShareAudit(actor, workspaceId, documentId, recipientId, "knowledge.context-share.revoked");
        return contextShare(access.tenantId(), workspaceId, documentId, recipientId);
    }

    @Override
    public ScopedKnowledgeSearchResult searchForScopedContext(ActorContext actor, UUID workspaceId, String query, int topK) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("多来源正式知识检索仅接受本人 HUMAN 身份。");
        if (blank(query)) throw EafException.invalid("检索 query 不能为空且不能超过 Embedding 单项限制。");
        if (topK < 1 || topK > SEARCH_TOP_K_MAX) throw EafException.invalid("topK 必须在 1-10 之间。");
        // 此 Owner API 可被其他模块直接调用，不能只依赖 Context 入口替它检查来源资格。
        if (!identities.isActiveHuman(actor.tenantId(), actor.actorId())) throw EafException.notFound();
        workspaces.require(actor, workspaceId, "context:read");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        if (query.codePointCount(0, query.length()) > profile.maxTextCodePoints())
            throw EafException.invalid("检索 query 不能为空且不能超过 Embedding 单项限制。");
        requireExternalEmbedding(actor, workspaceId, profile);
        var attribution = searchScope(actor, access, null);
        var result = embeddings.embed(new EmbeddingRequest(profile.provider(), profile.model(), profile.revision(),
                "EMBEDDING_QUERY", List.of(query), profile.dimension(), Math.min(EMBEDDING_INPUT_TOKENS, profile.maxInputTokens()),
                Instant.now(clock).plusSeconds(30), attribution));
        validateQueryEmbedding(result, profile);
        var hits = scopedVectorCandidates(vectorLiteral(result.vectors().getFirst()), access, actor, profile, topK);
        var projected = hits.stream().map(item -> new ScopedKnowledgeSearchHit(
                withRetrieval(item.hit(), List.of("VECTOR"), null, null, null, null), item.accessPath(),
                item.shareId(), item.shareVersion())).toList();
        return new ScopedKnowledgeSearchResult(topK, projected);
    }

    @Override
    public boolean isScopedContextHitUsable(ActorContext actor, UUID workspaceId, ScopedKnowledgeSearchHit scopedHit) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated() || scopedHit == null
                || scopedHit.hit() == null || !workspaceId.equals(scopedHit.hit().workspaceId())) return false;
        if (!identities.isActiveHuman(actor.tenantId(), actor.actorId())) return false;
        var hit = scopedHit.hit();
        workspaces.require(actor, workspaceId, "context:read");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        String accessPath = scopedHit.accessPath();
        if ("DIRECT".equals(accessPath)) {
            var count = jdbc.queryForObject("select count(*) from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? and v.status = 'PUBLISHED' join knowledge.chunk c on c.tenant_id = v.tenant_id and c.workspace_id = v.workspace_id and c.document_id = v.document_id and c.asset_version = v.asset_version and c.id = ? and c.content_hash = ? join knowledge.index_build b on b.id = ? and b.tenant_id = d.tenant_id and b.workspace_id = d.workspace_id and b.document_id = d.id and b.asset_version = v.asset_version and b.status = 'READY' join knowledge.document_publication p on p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id and p.document_id = b.document_id and p.asset_version = b.asset_version and p.build_id = b.id and p.status = 'ACTIVE' where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')"
                            + managedSourceReadGuard(),
                    Integer.class, hit.documentVersion(), hit.chunkId(), hit.contentHash(), hit.buildId(), access.tenantId(), workspaceId,
                    hit.documentId(), actor.actorId(), actor.actorId());
            return count != null && count == 1 && scopedHit.shareId() == null && scopedHit.shareVersion() == null;
        }
        if (!"SHARE".equals(accessPath) || scopedHit.shareId() == null || scopedHit.shareVersion() == null) return false;
        // 正式上下文共享本身就是第二条授权路径；这里核验精确共享记录，不要求接收者另有文档直读权。
        var count = jdbc.queryForObject("select count(*) from knowledge.context_share s join knowledge.document d on d.tenant_id = s.tenant_id and d.workspace_id = s.workspace_id and d.id = s.document_id and d.status = 'PUBLISHED' join knowledge.document_version v on v.tenant_id = s.tenant_id and v.workspace_id = s.workspace_id and v.document_id = s.document_id and v.asset_version = s.document_version and v.status = 'PUBLISHED' join knowledge.document_publication p on p.tenant_id = s.tenant_id and p.workspace_id = s.workspace_id and p.document_id = s.document_id and p.asset_version = s.document_version and p.build_id = s.build_id and p.status = 'ACTIVE' join knowledge.index_build b on b.id = s.build_id and b.tenant_id = s.tenant_id and b.workspace_id = s.workspace_id and b.document_id = s.document_id and b.asset_version = s.document_version and b.status = 'READY' join knowledge.chunk c on c.tenant_id = b.tenant_id and c.workspace_id = b.workspace_id and c.document_id = b.document_id and c.asset_version = b.asset_version and c.id = ? and c.content_hash = ? where s.id = ? and s.row_version = ? and s.status = 'ACTIVE' and s.tenant_id = ? and s.workspace_id = ? and s.document_id = ? and s.recipient_id = ? and s.document_version = ? and s.build_id = ? and s.content_hash = v.content_hash"
                        + managedSourceReadGuard(),
                Integer.class, hit.chunkId(), hit.contentHash(), scopedHit.shareId(), scopedHit.shareVersion(), access.tenantId(), workspaceId,
                hit.documentId(), actor.actorId(), hit.documentVersion(), hit.buildId(), actor.actorId());
        return count != null && count == 1;
    }

    @Override
    public KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK) {
        return search(actor, workspaceId, query, topK, null);
    }

    @Override
    public KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK,
                                       KnowledgeSearchScope usageScope) {
        return search(actor, workspaceId, query, topK, usageScope, "VECTOR");
    }

    @Override
    public KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK,
                                       KnowledgeSearchScope usageScope, String mode) {
        if (blank(query))
            throw EafException.invalid("检索 query 不能为空且不能超过 Embedding 单项限制。");
        if (topK < 1 || topK > SEARCH_TOP_K_MAX) throw EafException.invalid("topK 必须在 1-10 之间。");
        if (!List.of("VECTOR", "HYBRID").contains(mode)) throw EafException.invalid("检索 mode 必须为 VECTOR 或 HYBRID。");
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        var delegatedScope = mcpDocumentScope(actor, workspaceId);
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        if (query.codePointCount(0, query.length()) > profile.maxTextCodePoints())
            throw EafException.invalid("检索 query 不能为空且不能超过 Embedding 单项限制。");
        requireExternalEmbedding(actor, workspaceId, profile);
        var attribution = searchScope(actor, access, usageScope);
        // 查询向量和索引使用兼容的 Provider Profile；授权过滤发生在两路召回 SQL 内。
        var result = embeddings.embed(new EmbeddingRequest(profile.provider(), profile.model(), profile.revision(),
                "EMBEDDING_QUERY", List.of(query), profile.dimension(), Math.min(EMBEDDING_INPUT_TOKENS, profile.maxInputTokens()),
                Instant.now(clock).plusSeconds(30), attribution));
        validateQueryEmbedding(result, profile);
        var vector = vectorLiteral(result.vectors().get(0));
        var vectorHits = vectorCandidates(vector, access, actor, profile, "VECTOR".equals(mode) ? topK : 20,
                delegatedScope);
        if ("VECTOR".equals(mode)) {
            var hits = new ArrayList<KnowledgeSearchHit>();
            for (var i = 0; i < vectorHits.size(); i++) hits.add(withRetrieval(vectorHits.get(i), List.of("VECTOR"), i + 1,
                    null, null, null));
            return new KnowledgeSearchResult(topK, List.copyOf(hits), mode);
        }
        var tokens = lexicalTokens(query);
        var tsQuery = tokens.isEmpty() ? null : String.join(" | ", tokens);
        var lexicalHits = tsQuery == null ? List.<KnowledgeSearchHit>of()
                : lexicalCandidates(tsQuery, vector, access, actor, profile, delegatedScope);
        var merged = new java.util.LinkedHashMap<SearchKey, SearchMerge>();
        for (var i = 0; i < vectorHits.size(); i++) {
            var hit = vectorHits.get(i);
            var entry = merged.computeIfAbsent(new SearchKey(hit.buildId(), hit.chunkId()), ignored -> new SearchMerge(hit));
            entry.vectorRank = i + 1;
            entry.score += 1.0d / (60 + i + 1);
        }
        for (var i = 0; i < lexicalHits.size(); i++) {
            var hit = lexicalHits.get(i);
            var entry = merged.computeIfAbsent(new SearchKey(hit.buildId(), hit.chunkId()), ignored -> new SearchMerge(hit));
            entry.lexicalRank = i + 1;
            entry.score += 1.0d / (60 + i + 1);
        }
        var fused = merged.values().stream().sorted(java.util.Comparator
                        .comparingDouble((SearchMerge item) -> item.score).reversed()
                        .thenComparing(item -> item.hit.documentId().toString())
                        .thenComparingInt(item -> item.hit.chunkOrder())
                        .thenComparing(item -> item.hit.chunkId().toString()))
                .limit(topK).map(item -> withRetrieval(item.hit,
                        item.vectorRank == null ? List.of("LEXICAL") : item.lexicalRank == null
                                ? List.of("VECTOR") : List.of("VECTOR", "LEXICAL"),
                        item.vectorRank, item.lexicalRank, item.score,
                        item.lexicalRank == null ? null : "p9-lexical-1"))
                .toList();
        return new KnowledgeSearchResult(topK, fused, mode);
    }

    private List<ScopedKnowledgeSearchHit> scopedVectorCandidates(String vector, WorkspaceAccess access,
                                                                  ActorContext actor, EmbeddingProfile profile,
                                                                  int limit) {
        // 授权路径在候选 SQL 内判定；未共享文档不会先进入向量候选集。
        return jdbc.query("select c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.id chunk_id, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text, e.build_id, e.configuration_signature, e.distance_metric, e.embedding <=> ?::public.vector as distance, "
                        + "case when exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') then 'DIRECT' else 'SHARE' end access_path, "
                        + "case when exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') then null else s.id end share_id, "
                        + "case when exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') then null else s.row_version end share_version "
                        + "from knowledge.embedding e join knowledge.chunk c on c.tenant_id = e.tenant_id and c.workspace_id = e.workspace_id and c.document_id = e.document_id and c.asset_version = e.asset_version and c.id = e.chunk_id "
                        + "join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id "
                        + "join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = c.asset_version "
                        + "join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.asset_version = e.asset_version and p.build_id = e.build_id and p.status = 'ACTIVE' "
                        + "join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id and b.workspace_id = e.workspace_id and b.document_id = e.document_id and b.dimension = e.dimension and b.configuration_signature = e.configuration_signature and b.status = 'READY' "
                        + "left join lateral (select cs.id, cs.row_version from knowledge.context_share cs where cs.tenant_id = d.tenant_id and cs.workspace_id = d.workspace_id and cs.document_id = d.id and cs.recipient_id = ? and cs.status = 'ACTIVE' and cs.document_version = v.asset_version and cs.build_id = p.build_id and cs.content_hash = v.content_hash order by cs.id limit 1) s on true "
                        + "where e.tenant_id = ? and e.workspace_id = ? and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and (s.id is not null or exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE'))"
                        + managedSourceReadGuard() + " order by distance, c.document_id, c.chunk_order, c.id limit ?",
                (rs, rowNum) -> new ScopedKnowledgeSearchHit(mapSearchHit(rs), rs.getString("access_path"),
                        rs.getObject("share_id", UUID.class), (Long) rs.getObject("share_version")),
                vector, actor.actorId(), actor.actorId(), actor.actorId(), actor.actorId(),
                access.tenantId(), access.workspaceId(), profile.provider(), profile.model(), profile.revision(),
                profile.dimension(), actor.actorId(), actor.actorId(), limit);
    }

    private void requireHumanContextShareActor(ActorContext actor, UUID workspaceId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("正式知识上下文共享仅接受本人 HUMAN 身份。");
        if (workspaceId == null) throw EafException.notFound();
    }

    private void requireDocumentOwner(UUID tenantId, UUID workspaceId, UUID documentId, UUID actorId) {
        var count = jdbc.queryForObject("select count(*) from knowledge.document where tenant_id = ? and workspace_id = ? and id = ? and owner_id = ?",
                Integer.class, tenantId, workspaceId, documentId, actorId);
        if (count == null || count != 1) throw EafException.notFound();
    }

    private ContextShare contextShare(UUID tenantId, UUID workspaceId, UUID documentId, UUID recipientId) {
        var share = jdbc.query("select id, recipient_id, document_version, build_id, content_hash, status, row_version, created_by, created_at, updated_at "
                        + "from knowledge.context_share where tenant_id = ? and workspace_id = ? and document_id = ? and recipient_id = ?",
                rs -> rs.next() ? mapContextShare(rs, tenantId, workspaceId, documentId) : null,
                tenantId, workspaceId, documentId, recipientId);
        return share == null ? null : withValidity(share, tenantId);
    }

    private ContextShare mapContextShare(java.sql.ResultSet rs, UUID tenantId, UUID workspaceId, UUID documentId)
            throws java.sql.SQLException {
        return new ContextShare(rs.getObject("id", UUID.class), tenantId, workspaceId, documentId,
                rs.getObject("recipient_id", UUID.class), rs.getInt("document_version"),
                rs.getObject("build_id", UUID.class), rs.getString("content_hash").trim(), rs.getString("status"),
                rs.getLong("row_version"), rs.getObject("created_by", UUID.class),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(), false, null);
    }

    private ContextShare withValidity(ContextShare share, UUID tenantId) {
        if (!"ACTIVE".equals(share.status())) return new ContextShare(share.id(), share.tenantId(), share.workspaceId(),
                share.documentId(), share.recipientId(), share.documentVersion(), share.buildId(), share.contentHash(),
                share.status(), share.rowVersion(), share.createdBy(), share.createdAt(), share.updatedAt(), false, "REVOKED");
        var binding = jdbc.query("select p.asset_version, p.build_id, v.content_hash from knowledge.document_publication p "
                        + "join knowledge.document_version v on v.tenant_id = p.tenant_id and v.workspace_id = p.workspace_id "
                        + "and v.document_id = p.document_id and v.asset_version = p.asset_version and v.status = 'PUBLISHED' "
                        + "join knowledge.index_build b on b.id = p.build_id and b.status = 'READY' "
                        + "where p.tenant_id = ? and p.workspace_id = ? and p.document_id = ? and p.status = 'ACTIVE'",
                rs -> rs.next() ? new PublicationBinding(rs.getInt(1), rs.getObject(2, UUID.class), rs.getString(3).trim()) : null,
                share.tenantId(), share.workspaceId(), share.documentId());
        if (binding == null || binding.assetVersion() != share.documentVersion()
                || !binding.buildId().equals(share.buildId()) || !binding.contentHash().equals(share.contentHash()))
            return invalidShare(share, "PUBLICATION_CHANGED");
        if (!identities.isActiveHuman(tenantId, share.recipientId())) return invalidShare(share, "RECIPIENT_INACTIVE");
        if (!workspaces.isAuthorized(tenantId, share.recipientId(), share.workspaceId(), "context:read")
                || !workspaces.isAuthorized(tenantId, share.recipientId(), share.workspaceId(), "knowledge:read"))
            return invalidShare(share, "SOURCE_ACCESS_REVOKED");
        return new ContextShare(share.id(), share.tenantId(), share.workspaceId(), share.documentId(), share.recipientId(),
                share.documentVersion(), share.buildId(), share.contentHash(), share.status(), share.rowVersion(),
                share.createdBy(), share.createdAt(), share.updatedAt(), true, null);
    }

    private ContextShare invalidShare(ContextShare share, String reason) {
        return new ContextShare(share.id(), share.tenantId(), share.workspaceId(), share.documentId(), share.recipientId(),
                share.documentVersion(), share.buildId(), share.contentHash(), share.status(), share.rowVersion(),
                share.createdBy(), share.createdAt(), share.updatedAt(), false, reason);
    }

    private void appendContextShareAudit(ActorContext actor, UUID workspaceId, UUID documentId, UUID recipientId,
                                         String action) {
        audit.append(new AuditFact(UUID.randomUUID().toString(), actor.tenantId(), workspaceId, actor.actorId(), null,
                action, "CHANGED", "{\"documentId\":\"" + documentId + "\",\"recipientId\":\""
                + recipientId + "\"}", null));
    }

    private record PublicationBinding(int assetVersion, UUID buildId, String contentHash) { }

    private List<KnowledgeSearchHit> vectorCandidates(String vector, WorkspaceAccess access, ActorContext actor,
                                                      EmbeddingProfile profile, int limit,
                                                      McpReadonlyDelegationScope delegatedScope) {
        // build、向量维度和配置签名必须完全相同；不同向量空间绝不参与同一排序。
        var parameters = new ArrayList<Object>(List.of(vector, access.tenantId(), access.workspaceId(), profile.provider(),
                profile.model(), profile.revision(), profile.dimension(), actor.actorId()));
        var sql = "select c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.id chunk_id, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text, e.build_id, e.configuration_signature, e.distance_metric, e.embedding <=> ?::public.vector as distance from knowledge.embedding e join knowledge.chunk c on c.tenant_id = e.tenant_id and c.workspace_id = e.workspace_id and c.document_id = e.document_id and c.asset_version = e.asset_version and c.id = e.chunk_id join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = c.asset_version join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.asset_version = e.asset_version and p.build_id = e.build_id join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id and b.workspace_id = e.workspace_id and b.document_id = e.document_id and b.dimension = e.dimension and b.configuration_signature = e.configuration_signature and b.status = 'READY' where e.tenant_id = ? and e.workspace_id = ? and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and p.status = 'ACTIVE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')"
                + delegatedKnowledgeFilter(delegatedScope, parameters) + managedSourceReadGuard();
        parameters.add(actor.actorId());
        if (delegatedScope != null) {
            sql += managedSourceReadGuard();
            parameters.add(delegatedScope.ownerId());
        }
        sql += " order by distance, c.document_id, c.chunk_order, c.id limit ?";
        parameters.add(limit);
        return jdbc.query(sql, (rs, rowNum) -> mapSearchHit(rs), parameters.toArray());
    }

    private List<KnowledgeSearchHit> lexicalCandidates(String tsQuery, String vector, WorkspaceAccess access,
                                                       ActorContext actor, EmbeddingProfile profile,
                                                       McpReadonlyDelegationScope delegatedScope) {
        var parameters = new ArrayList<Object>(List.of(vector, tsQuery, access.tenantId(), access.workspaceId(), tsQuery,
                profile.provider(), profile.model(), profile.revision(), profile.dimension(), actor.actorId()));
        var sql = "select c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.id chunk_id, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text, e.build_id, e.configuration_signature, e.distance_metric, e.embedding <=> ?::public.vector as distance, ts_rank_cd(le.search_vector, ?::tsquery) as lexical_score from knowledge.lexical_entry le join knowledge.embedding e on e.tenant_id = le.tenant_id and e.workspace_id = le.workspace_id and e.document_id = le.document_id and e.asset_version = le.asset_version and e.build_id = le.build_id and e.chunk_id = le.chunk_id join knowledge.chunk c on c.tenant_id = e.tenant_id and c.workspace_id = e.workspace_id and c.document_id = e.document_id and c.asset_version = e.asset_version and c.id = e.chunk_id join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = c.asset_version join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.asset_version = e.asset_version and p.build_id = e.build_id join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id and b.workspace_id = e.workspace_id and b.document_id = e.document_id and b.dimension = e.dimension and b.configuration_signature = e.configuration_signature and b.status = 'READY' where le.tenant_id = ? and le.workspace_id = ? and le.normalization_version = 'p9-lexical-1' and le.search_vector @@ ?::tsquery and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and p.status = 'ACTIVE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')"
                + delegatedKnowledgeFilter(delegatedScope, parameters) + managedSourceReadGuard();
        parameters.add(actor.actorId());
        if (delegatedScope != null) {
            sql += managedSourceReadGuard();
            parameters.add(delegatedScope.ownerId());
        }
        sql += " order by lexical_score desc, c.document_id, c.chunk_order, c.id limit 20";
        return jdbc.query(sql, (rs, rowNum) -> mapSearchHit(rs), parameters.toArray());
    }

    private String delegatedKnowledgeFilter(McpReadonlyDelegationScope scope, List<Object> parameters) {
        if (scope == null) return "";
        var documents = scope.knowledgeDocumentIds().stream().sorted().toList();
        if (documents.isEmpty()) return " and false";
        parameters.addAll(documents);
        parameters.add(scope.ownerId());
        return " and d.id in (" + String.join(",", java.util.Collections.nCopies(documents.size(), "?")) + ")"
                + " and exists (select 1 from knowledge.document_permission dp_owner where dp_owner.tenant_id = d.tenant_id "
                + "and dp_owner.workspace_id = d.workspace_id and dp_owner.document_id = d.id and dp_owner.actor_id = ? "
                + "and dp_owner.action = 'knowledge:read' and dp_owner.status = 'ACTIVE')";
    }

    private McpReadonlyDelegationScope mcpDocumentScope(ActorContext actor, UUID workspaceId) {
        if (actor == null || !IdentityService.MCP_AUDIENCE.equals(actor.delegationAudience())) return null;
        if (delegationIdentities == null) throw EafException.forbidden("MCP 委托知识范围无法复核。");
        return delegationIdentities.mcpReadonlyScope(actor)
                .filter(scope -> scope.workspaceId().equals(workspaceId))
                .orElseThrow(() -> EafException.forbidden("MCP 委托知识范围已失效。"));
    }

    private boolean hasDocumentReadPermission(UUID actorId, UUID tenantId, UUID workspaceId, UUID documentId) {
        var count = jdbc.queryForObject("select count(*) from knowledge.document_permission where tenant_id = ? and workspace_id = ? and document_id = ? and actor_id = ? and action = 'knowledge:read' and status = 'ACTIVE'",
                Integer.class, tenantId, workspaceId, documentId, actorId);
        return count != null && count > 0;
    }

    private KnowledgeSearchHit mapSearchHit(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new KnowledgeSearchHit(rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("document_id", UUID.class), rs.getInt("asset_version"), rs.getObject("chunk_id", UUID.class),
                rs.getString("source_ref"), rs.getString("chunking_version"), rs.getInt("chunk_order"),
                rs.getInt("start_offset"), rs.getInt("end_offset"), rs.getString("offset_unit"), rs.getString("content"),
                rs.getString("content_hash"), rs.getObject("build_id", UUID.class), rs.getString("configuration_signature"),
                rs.getString("distance_metric"), rs.getDouble("distance"), stringList(rs.getString("heading_path")));
    }

    private KnowledgeSearchHit withRetrieval(KnowledgeSearchHit hit, List<String> channels, Integer vectorRank,
                                              Integer lexicalRank, Double score, String normalizationVersion) {
        return new KnowledgeSearchHit(hit.tenantId(), hit.workspaceId(), hit.documentId(), hit.documentVersion(),
                hit.chunkId(), hit.sourceRef(), hit.chunkingVersion(), hit.chunkOrder(), hit.startOffset(), hit.endOffset(),
                hit.offsetUnit(), hit.content(), hit.contentHash(), hit.buildId(), hit.configurationSignature(),
                hit.distanceMetric(), hit.distance(), hit.headingPath(), channels, vectorRank, lexicalRank, score,
                normalizationVersion);
    }

    private List<String> lexicalTokens(String text) {
        var normalized = Normalizer.normalize(text, Normalizer.Form.NFKC).toLowerCase(java.util.Locale.ROOT);
        var tokens = new java.util.LinkedHashSet<String>();
        var identifierMatcher = Pattern.compile("[\\p{L}\\p{N}]+(?:[-_./][\\p{L}\\p{N}]+)+").matcher(normalized);
        while (identifierMatcher.find()) {
            var identifier = identifierMatcher.group();
            tokens.add(identifier.replaceAll("[^\\p{L}\\p{N}_]", "_"));
            for (var part : identifier.split("[-_./]+")) if (!part.isBlank()) tokens.add(part);
        }
        var points = normalized.codePoints().toArray();
        for (var index = 0; index < points.length;) {
            var script = Character.UnicodeScript.of(points[index]);
            if (script == Character.UnicodeScript.HAN) {
                var start = index;
                while (index < points.length && Character.UnicodeScript.of(points[index]) == Character.UnicodeScript.HAN) index++;
                if (index - start == 1) tokens.add(new String(points, start, 1));
                else for (var i = start; i < index - 1; i++) tokens.add(new String(points, i, 2));
            } else if (Character.isLetterOrDigit(points[index])) {
                var start = index++;
                while (index < points.length && Character.isLetterOrDigit(points[index])
                        && Character.UnicodeScript.of(points[index]) != Character.UnicodeScript.HAN) index++;
                tokens.add(new String(points, start, index - start));
            } else index++;
            if (tokens.size() >= 64) break;
        }
        return tokens.stream().filter(token -> token.matches("[\\p{L}\\p{N}_]+"))
                .limit(64).toList();
    }

    private void validateQueryEmbedding(EmbeddingResult result, EmbeddingProfile profile) {
        if (result == null || !profile.provider().equals(result.provider()) || !profile.model().equals(result.model())
                || result.dimension() != profile.dimension() || result.vectors() == null || result.vectors().size() != 1
                || !"KNOWN".equals(result.usageStatus()) || result.inputTokens() < 0 || result.inputTokens() > profile.maxInputTokens())
            throw new IllegalStateException("查询 Embedding 返回与索引配置不一致。");
        var vector = result.vectors().get(0);
        if (vector == null || vector.size() != profile.dimension() || vector.stream().anyMatch(value -> value == null || !Float.isFinite(value)))
            throw new IllegalStateException("查询 Embedding 返回非法维度或数值。");
    }

    private void validatePublicationCommand(long expectedRowVersion, UUID buildId, String idempotencyKey) {
        if (expectedRowVersion <= 0 || buildId == null || blank(idempotencyKey) || idempotencyKey.length() > 200 || hasControl(idempotencyKey))
            throw EafException.invalid("发布命令需要有效 expectedRowVersion、buildId 和幂等键。");
    }

    private Optional<DocumentState> lockDocumentForPublication(WorkspaceAccess access, ActorContext actor, UUID documentId) {
        return jdbc.query("select d.row_version, d.status from knowledge.document d where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:publish' and p.status = 'ACTIVE') for update",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<DocumentState>>) rs -> rs.next() ? Optional.of(new DocumentState(rs.getLong("row_version"), rs.getString("status"))) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, actor.actorId());
    }

    private Optional<DocumentState> lockDocumentForVersion(WorkspaceAccess access, ActorContext actor, UUID documentId) {
        return jdbc.query("select d.row_version, d.status from knowledge.document d where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:write' and p.status = 'ACTIVE') and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE') for update",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<DocumentState>>) rs -> rs.next() ? Optional.of(new DocumentState(rs.getLong("row_version"), rs.getString("status"))) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, actor.actorId(), actor.actorId());
    }

    private VersionRequest findVersionByIdempotency(WorkspaceAccess access, UUID documentId, String key) {
        return jdbc.query("select asset_version, request_hash from knowledge.document_version where tenant_id = ? and workspace_id = ? and document_id = ? and idempotency_key = ?",
                rs -> rs.next() ? new VersionRequest(rs.getInt("asset_version"), rs.getString("request_hash")) : null,
                access.tenantId(), access.workspaceId(), documentId, key);
    }

    private Integer currentVersion(WorkspaceAccess access, UUID documentId) {
        return jdbc.query("select asset_version from knowledge.document_publication where tenant_id = ? and workspace_id = ? and document_id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? rs.getInt("asset_version") : null, access.tenantId(), access.workspaceId(), documentId);
    }

    private void appendPublicationOutbox(KnowledgePublication publication) {
        try {
            var payload = new TreeMap<String, Object>();
            payload.put("eventId", publication.eventId());
            payload.put("tenantId", publication.tenantId());
            payload.put("workspaceId", publication.workspaceId());
            payload.put("documentId", publication.documentId());
            payload.put("assetVersion", publication.assetVersion());
            payload.put("buildId", publication.buildId());
            payload.put("action", publication.action());
            payload.put("releaseOrigin", publication.releaseOrigin());
            payload.put("candidateId", publication.candidateId());
            payload.put("candidateRevision", publication.candidateRevision());
            payload.put("baseVersion", publication.baseVersion());
            payload.put("contentHash", publication.contentHash());
            var eventType = "PUBLISHED".equals(publication.action())
                    ? "eaf.knowledge.version-published.v1" : "eaf.knowledge.version-revoked.v1";
            // Outbox 与发布事实处于同一事务，后续消费者只能依据这份固定快照恢复。
            jdbc.update("insert into knowledge.outbox(event_id, event_type, payload) values (?, ?, ?::jsonb)",
                    publication.eventId(), eventType, mapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Knowledge 发布事件无法序列化。", e);
        }
    }

    private Optional<KnowledgePublication> findPublicationEvent(WorkspaceAccess access, UUID documentId, String idempotencyKey) {
        return jdbc.query("select id, tenant_id, workspace_id, document_id, asset_version, build_id, action, document_status, document_row_version, occurred_at, release_origin, candidate_id, candidate_revision, base_version, content_hash from knowledge.publication_event where tenant_id = ? and workspace_id = ? and document_id = ? and idempotency_key = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgePublication>>) rs -> rs.next() ? Optional.of(mapPublicationEvent(rs)) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, idempotencyKey);
    }

    private KnowledgePublication sameCommand(KnowledgePublication event, String requestHash) {
        var stored = jdbc.queryForObject("select request_hash from knowledge.publication_event where id = ?", String.class, event.eventId());
        if (!requestHash.equals(stored)) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "幂等键已用于不同发布命令。");
        return event;
    }

    private KnowledgePublication mapPublicationEvent(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new KnowledgePublication(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("document_id", UUID.class), rs.getInt("asset_version"), rs.getObject("build_id", UUID.class), rs.getString("action"),
                rs.getString("document_status"), rs.getLong("document_row_version"), rs.getTimestamp("occurred_at").toInstant(),
                rs.getString("release_origin"), rs.getObject("candidate_id", UUID.class),
                rs.getObject("candidate_revision", Integer.class), rs.getObject("base_version", Integer.class), rs.getString("content_hash"));
    }

    private void processBuild(UUID buildId, ActorContext actor, WorkspaceAccess access, UUID documentId) {
        try {
            var profile = embeddings.profile();
            requireExternalEmbedding(actor, access.workspaceId(), profile);
            var build = jdbc.queryForObject("select id, asset_version, chunking_version, provider, model, model_revision, dimension, configuration_signature, max_batch_size, max_input_tokens, max_text_code_points, retry_generation, total_chunks, next_chunk_order from knowledge.index_build where id = ? and tenant_id = ? and workspace_id = ? and document_id = ?",
                    (rs, rowNum) -> new BuildWork(rs.getObject("id", UUID.class), rs.getInt("asset_version"), rs.getString("chunking_version"),
                            rs.getString("provider"), rs.getString("model"), rs.getString("model_revision"), rs.getInt("dimension"),
                            rs.getString("configuration_signature"), rs.getInt("max_batch_size"), rs.getInt("max_input_tokens"),
                            rs.getInt("max_text_code_points"), rs.getInt("retry_generation"), rs.getInt("total_chunks"), rs.getInt("next_chunk_order")),
                    buildId, access.tenantId(), access.workspaceId(), documentId);
            if (!profile.provider().equals(build.provider()) || !profile.model().equals(build.model())
                    || !profile.revision().equals(build.modelRevision()) || profile.dimension() != build.dimension()
                    || profile.maxBatchSize() != build.maxBatchSize() || profile.maxInputTokens() != build.maxInputTokens()
                    || profile.maxTextCodePoints() != build.maxTextCodePoints())
                throw new EmbeddingFailure("PROFILE_UNAVAILABLE", "索引构建固定的 Embedding Profile 当前不可用。", false, false);
            var lexicalIndexVersion = jdbc.queryForObject("select lexical_index_version from knowledge.index_build where id = ?", String.class, build.id());
            if ("p9-lexical-1".equals(lexicalIndexVersion))
                ensureLexicalEntries(access, documentId, build.id(), build.assetVersion(), build.chunkingVersion());
            var chunks = jdbc.query("select c.id, c.chunk_order, c.content, c.content_hash, c.heading_path::text from knowledge.chunk c where c.tenant_id = ? and c.workspace_id = ? and c.document_id = ? and c.asset_version = ? and c.chunking_version = ? and c.chunk_order >= ? and not exists (select 1 from knowledge.embedding e where e.tenant_id = c.tenant_id and e.workspace_id = c.workspace_id and e.document_id = c.document_id and e.asset_version = c.asset_version and e.chunk_id = c.id and e.build_id = ? and e.configuration_signature = ?) order by c.chunk_order",
                    (rs, rowNum) -> new ChunkWork(rs.getObject("id", UUID.class), rs.getInt("chunk_order"), rs.getString("content"), rs.getString("content_hash"), stringList(rs.getString("heading_path"))),
                    access.tenantId(), access.workspaceId(), documentId, build.assetVersion(), build.chunkingVersion(),
                    build.nextChunkOrder(), build.id(), build.configurationSignature());
            for (var offset = 0; offset < chunks.size();) {
                // 每个批次重查知识写入与外发授权；稳定键绑定首个 Chunk，恢复时不会重发已完成批次。
                workspaces.require(actor, access.workspaceId(), "knowledge:write");
                requireExternalEmbedding(actor, access.workspaceId(), profile);
                var end = offset;
                var batchTokens = 0;
                while (end < chunks.size() && end - offset < build.maxBatchSize()) {
                    var embeddingText = embeddingText(chunks.get(end));
                    if (embeddingText.codePointCount(0, embeddingText.length()) > build.maxTextCodePoints())
                        throw new EmbeddingFailure("INPUT_LIMIT_EXCEEDED", "Chunk 超过构建固定的单项输入上限。", false, false);
                    var chunkTokens = Math.max(1, (embeddingText.codePointCount(0, embeddingText.length()) + 3) / 4);
                    if (batchTokens + chunkTokens > build.maxInputTokens()) break;
                    batchTokens += chunkTokens;
                    end++;
                }
                if (end == offset) throw new EmbeddingFailure("INPUT_LIMIT_EXCEEDED", "单个 Chunk 超过当前 Embedding Profile 输入上限。", false, false);
                var batch = chunks.subList(offset, end);
                var callKey = "job:embedding:" + build.id() + ":attempt:" + build.retryGeneration() + ":start:" + batch.getFirst().order();
                var callScope = new EmbeddingCallScope(access.tenantId(), access.workspaceId(), null, null, "USER",
                        "JOB", build.id(), batch.getFirst().order(), callKey, "EMBEDDING_BATCH");
                var result = embeddings.embed(new EmbeddingRequest(build.provider(), build.model(), build.modelRevision(),
                        "EMBEDDING_BATCH", batch.stream().map(this::embeddingText).toList(), build.dimension(),
                        build.maxInputTokens(), Instant.now(clock).plusSeconds(30), callScope));
                validateEmbeddingResult(result, batch.size(), build, profile);
                var now = Timestamp.from(Instant.now(clock));
                for (var i = 0; i < batch.size(); i++) {
                    var chunk = batch.get(i);
                    jdbc.update("insert into knowledge.embedding(id, tenant_id, workspace_id, document_id, asset_version, chunk_id, build_id, provider, model, model_revision, dimension, chunking_version, distance_metric, configuration_signature, embedding, created_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'COSINE', ?, ?::public.vector, ?) on conflict (tenant_id, workspace_id, chunk_id, configuration_signature) do nothing",
                            UUID.randomUUID(), access.tenantId(), access.workspaceId(), documentId, build.assetVersion(), chunk.id(), build.id(),
                            build.provider(), build.model(), build.modelRevision(), build.dimension(), build.chunkingVersion(), build.configurationSignature(), vectorLiteral(result.vectors().get(i)), now);
                }
                var next = batch.get(batch.size() - 1).order() + 1;
                jdbc.update("update knowledge.index_build set completed_chunks = (select count(*) from knowledge.embedding where build_id = ?), next_chunk_order = ?, input_tokens = input_tokens + ?, embedding_calls = embedding_calls + 1, updated_at = ? where id = ? and status = 'INDEXING'",
                        build.id(), next, result.inputTokens(), now, build.id());
                offset = end;
            }
            var completed = jdbc.queryForObject("select count(*) from knowledge.embedding where build_id = ?", Integer.class, build.id());
            if (completed != null && completed == build.totalChunks()) {
                jdbc.update("update knowledge.index_build set status = 'READY', completed_chunks = ?, completed_at = ?, updated_at = ? where id = ? and status = 'INDEXING'",
                        completed, Timestamp.from(Instant.now(clock)), Timestamp.from(Instant.now(clock)), build.id());
            } else {
                throw new IllegalStateException("索引批次未覆盖全部 Chunk。");
            }
        } catch (EmbeddingFailure e) {
            failBuild(buildId, e.code());
        } catch (RuntimeException e) {
            failBuild(buildId, "INDEX_BUILD_FAILED");
        }
    }

    private void validateEmbeddingResult(io.eaf.model.api.EmbeddingResult result, int expected, BuildWork build,
                                         EmbeddingProfile profile) {
        if (result == null || !build.provider().equals(result.provider()) || !build.model().equals(result.model())
                || result.dimension() != build.dimension() || result.vectors() == null || result.vectors().size() != expected
                || !"KNOWN".equals(result.usageStatus()) || result.inputTokens() < 0 || result.inputTokens() > profile.maxInputTokens())
            throw new IllegalStateException("Embedding 返回与构建配置不一致。");
        result.vectors().forEach(vector -> {
            if (vector == null || vector.size() != build.dimension() || vector.stream().anyMatch(value -> value == null || !Float.isFinite(value)))
                throw new IllegalStateException("Embedding 返回非法维度或数值。");
        });
    }

    private void ensureLexicalEntries(WorkspaceAccess access, UUID documentId, UUID buildId, int assetVersion,
                                      String chunkingVersion) {
        var chunks = jdbc.query("select id, content, heading_path::text from knowledge.chunk where tenant_id = ? and workspace_id = ? and document_id = ? and asset_version = ? and chunking_version = ? order by chunk_order",
                (rs, row) -> new ChunkWork(rs.getObject("id", UUID.class), 0, rs.getString("content"), null,
                        stringList(rs.getString("heading_path"))), access.tenantId(), access.workspaceId(), documentId,
                assetVersion, chunkingVersion);
        var now = Timestamp.from(Instant.now(clock));
        for (var chunk : chunks) {
            var lexicalSource = String.join(" ", chunk.headingPath()) + "\n" + chunk.content();
            var normalized = String.join(" ", lexicalTokens(lexicalSource));
            jdbc.update("insert into knowledge.lexical_entry(tenant_id, workspace_id, document_id, asset_version, build_id, chunk_id, normalization_version, search_vector, created_at) values (?, ?, ?, ?, ?, ?, 'p9-lexical-1', to_tsvector('simple', ?), ?) on conflict (tenant_id, workspace_id, document_id, asset_version, build_id, chunk_id) do nothing",
                    access.tenantId(), access.workspaceId(), documentId, assetVersion, buildId, chunk.id(), normalized, now);
        }
    }

    private void failBuild(UUID buildId, String failureCode) {
        jdbc.update("update knowledge.index_build set status = 'FAILED', failure_code = ?, updated_at = ? where id = ? and status = 'INDEXING'",
                failureCode, Timestamp.from(Instant.now(clock)), buildId);
    }

    private KnowledgeIndexBuild getBuildForWrite(ActorContext actor, WorkspaceAccess access, UUID documentId, int assetVersion,
                                                 String idempotencyKey, String signature) {
        var priorKey = jdbc.query("select b.* from knowledge.index_build b where b.tenant_id = ? and b.workspace_id = ? and b.document_id = ? and b.asset_version = ? and b.idempotency_key = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<BuildIdentity>>) rs -> rs.next()
                        ? Optional.of(new BuildIdentity(mapBuild(rs), rs.getString("configuration_signature"))) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, assetVersion, idempotencyKey);
        if (priorKey.isPresent()) {
            if (!signature.equals(priorKey.get().signature()))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一索引幂等键不能绑定不同 Embedding Profile 或切块配置。");
            return priorKey.get().build();
        }
        return jdbc.query("select b.* from knowledge.index_build b where b.tenant_id = ? and b.workspace_id = ? and b.document_id = ? and b.asset_version = ? and b.configuration_signature = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeIndexBuild>>) rs -> rs.next() ? Optional.of(mapBuild(rs)) : Optional.empty(),
                access.tenantId(), access.workspaceId(), documentId, assetVersion, signature)
                .orElseThrow(() -> EafException.conflict("INDEX_BUILD_CONFLICT", "索引构建配置已被其他请求占用。"));
    }

    private Optional<BuildSource> findBuildSource(ActorContext actor, WorkspaceAccess access, UUID documentId, int assetVersion, String chunkingVersion) {
        return jdbc.query("select v.asset_version, count(c.id) total_chunks from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? left join knowledge.chunk c on c.tenant_id = v.tenant_id and c.workspace_id = v.workspace_id and c.document_id = v.document_id and c.asset_version = v.asset_version and c.chunking_version = ? where d.id = ? and d.tenant_id = ? and d.workspace_id = ? and d.status in ('DRAFT', 'PUBLISHED') and v.status in ('DRAFT', 'PUBLISHED') and exists (select 1 from knowledge.document_permission p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:write' and p.status = 'ACTIVE') group by v.asset_version",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<BuildSource>>) rs -> rs.next() ? Optional.of(new BuildSource(rs.getInt("asset_version"), rs.getInt("total_chunks"))) : Optional.empty(),
                assetVersion, chunkingVersion, documentId, access.tenantId(), access.workspaceId(), actor.actorId());
    }

    private String configurationSignature(String chunkingVersion, EmbeddingProfile profile) {
        if ("p9-structure-1".equals(chunkingVersion)) {
            return Hashing.sha256(String.join("|", profile.provider(), profile.model(), profile.revision(),
                    Integer.toString(profile.dimension()), Integer.toString(profile.maxBatchSize()),
                    Integer.toString(profile.maxInputTokens()), Integer.toString(profile.maxTextCodePoints()),
                    chunkingVersion, "heading-prefix-v1", "p9-lexical-1", "COSINE"));
        }
        return Hashing.sha256(String.join("|", profile.provider(), profile.model(), profile.revision(),
                Integer.toString(profile.dimension()), Integer.toString(profile.maxBatchSize()),
                Integer.toString(profile.maxInputTokens()), Integer.toString(profile.maxTextCodePoints()),
                chunkingVersion, "COSINE"));
    }

    private void requireExternalEmbedding(ActorContext actor, UUID workspaceId, EmbeddingProfile profile) {
        if (!"deterministic".equals(profile.provider()))
            workspaces.require(actor, workspaceId, "knowledge:external-embedding");
    }

    private void requireStoredDimension(EmbeddingProfile profile) {
        if (profile.dimension() > MAX_PGVECTOR_DIMENSION)
            throw EafException.conflict("EMBEDDING_SCHEMA_PROFILE_UNSUPPORTED",
                    "当前 pgvector 向量列仅支持 1-16000 维；拒绝越界 Profile，避免付费后才因存储维度失败。");
    }

    private EmbeddingCallScope searchScope(ActorContext actor, WorkspaceAccess access, KnowledgeSearchScope requested) {
        if (requested == null) {
            var jobId = UUID.randomUUID();
            return new EmbeddingCallScope(access.tenantId(), access.workspaceId(), null, null, "USER", "JOB", jobId, 0,
                    "job:embedding-query:" + jobId, "EMBEDDING_QUERY");
        }
        if (!access.tenantId().equals(requested.tenantId()) || !access.workspaceId().equals(requested.workspaceId())
                || !actor.actorId().equals(requested.actorId())
                || requested.scopeId() == null || requested.taskId() == null || requested.runId() == null
                || requested.callNo() < 0 || !List.of("USER", "EVALUATION").contains(requested.source())
                || !List.of("TASK", "WORKFLOW", "EVALUATION").contains(requested.scopeType()))
            throw EafException.forbidden("Knowledge Embedding 计量归属与已授权 Workspace 不一致。");
        return new EmbeddingCallScope(access.tenantId(), access.workspaceId(), requested.taskId(), requested.runId(),
                requested.source(), requested.scopeType(), requested.scopeId(), requested.callNo(),
                "task:" + requested.taskId() + ":run:" + requested.runId() + ":embedding-query", "EMBEDDING_QUERY");
    }

    private String vectorLiteral(List<Float> vector) { return "[" + vector.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(",")) + "]"; }

    private KnowledgeIndexBuild mapBuild(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new KnowledgeIndexBuild(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getObject("workspace_id", UUID.class),
                rs.getObject("document_id", UUID.class), rs.getInt("asset_version"), rs.getString("chunking_version"), rs.getString("provider"),
                rs.getString("model"), rs.getString("model_revision"), rs.getInt("dimension"), rs.getString("distance_metric"),
                rs.getString("configuration_signature"), rs.getString("status"), rs.getInt("total_chunks"), rs.getInt("completed_chunks"),
                rs.getInt("next_chunk_order"), rs.getInt("input_tokens"), rs.getInt("embedding_calls"), rs.getString("failure_code"),
                rs.getTimestamp("created_at").toInstant(), instant(rs, "started_at"), instant(rs, "completed_at"), rs.getTimestamp("updated_at").toInstant());
    }

    private Instant instant(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        var value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private Optional<Idempotent> findByIdempotency(ActorContext actor, UUID workspaceId, String key) {
        return jdbc.query("select id, request_hash from knowledge.document where tenant_id = ? and workspace_id = ? and owner_id = ? and idempotency_key = ?",
                rs -> rs.next() ? Optional.of(new Idempotent(rs.getObject("id", UUID.class), rs.getString("request_hash"))) : Optional.empty(),
                actor.tenantId(), workspaceId, actor.actorId(), key);
    }

    private KnowledgeDocument map(java.sql.ResultSet rs) throws java.sql.SQLException {
        try {
            Map<String, String> metadata = mapper.readValue(rs.getString("metadata"), new TypeReference<>() { });
            return new KnowledgeDocument(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class),
                    rs.getObject("workspace_id", UUID.class), rs.getObject("owner_id", UUID.class), rs.getString("title"),
                    rs.getString("source_ref"), Map.copyOf(metadata), rs.getInt("asset_version"), rs.getString("content"),
                    rs.getString("content_hash"), rs.getString("status"), rs.getTimestamp("created_at").toInstant(),
                    rs.getLong("row_version"));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("知识元数据存储格式损坏。", e);
        }
    }

    private void validate(CreateKnowledgeDocumentCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null)
            throw EafException.invalid("知识导入上下文不能为空。");
        if (blank(command.title()) || command.title().length() > 200) throw EafException.invalid("title 必须为 1-200 个字符。");
        if (blank(command.sourceRef()) || command.sourceRef().length() > 500 || hasControl(command.sourceRef()))
            throw EafException.invalid("sourceRef 必须为 1-500 个可记录字符。");
        if (blank(command.content())) throw EafException.invalid("content 不能为空。");
        if (command.content().getBytes(StandardCharsets.UTF_8).length > MAX_CONTENT_BYTES)
            throw EafException.invalid("content 超过 100 KiB 限制。");
        if (blank(command.idempotencyKey()) || command.idempotencyKey().length() > 200 || hasControl(command.idempotencyKey()))
            throw EafException.invalid("Idempotency-Key 必须为 1-200 个可记录字符。");
        var metadata = command.metadata() == null ? Map.<String, String>of() : command.metadata();
        if (metadata.size() > 16) throw EafException.invalid("metadata 最多包含 16 个键。");
        metadata.forEach((key, value) -> {
            if (key == null || !METADATA_KEY.matcher(key).matches() || value == null || value.length() > 256 || hasControl(value))
                throw EafException.invalid("metadata 只能包含有限的字符串键值。");
        });
    }

    private String metadataJson(Map<String, String> metadata) {
        try {
            return mapper.writeValueAsString(metadata);
        } catch (JsonProcessingException e) {
            throw EafException.invalid("metadata 无法编码。");
        }
    }

    private boolean blank(String value) { return value == null || value.isBlank(); }

    private boolean hasControl(String value) { return value.chars().anyMatch(Character::isISOControl); }

    private List<ChunkRange> split(String content, int maxCodePoints) {
        var codePoints = content.codePoints().toArray();
        var charOffsets = new int[codePoints.length + 1];
        for (var i = 0; i < codePoints.length; i++) charOffsets[i + 1] = charOffsets[i] + Character.charCount(codePoints[i]);
        var result = new ArrayList<ChunkRange>();
        var start = 0;
        var order = 1;
        while (start < codePoints.length) {
            var end = Math.min(start + maxCodePoints, codePoints.length);
            if (end < codePoints.length) {
                var paragraph = lastParagraphBoundary(codePoints, start, end);
                if (paragraph > start) end = paragraph;
                else {
                    var line = lastLineBoundary(codePoints, start, end);
                    if (line > start) end = line;
                }
                if (end > start && end < codePoints.length && codePoints[end - 1] == '\r' && codePoints[end] == '\n') end--;
            }
            // 空段只作为边界，不单独落库；前导空白并入后续正文，保证原文仍可完整重建。
            if (!hasNonWhitespace(codePoints, start, end)) {
                while (end < codePoints.length && Character.isWhitespace(codePoints[end])) end++;
                if (end < codePoints.length) end++;
            }
            result.add(new ChunkRange(order++, start, end, charOffsets[start], charOffsets[end], List.of()));
            start = end;
        }
        return result;
    }

    private List<ChunkRange> splitStructured(String content, int maxCodePoints) {
        var result = new ArrayList<ChunkRange>();
        var headingPath = new ArrayList<String>();
        var bodyStart = 0;
        var nextOrder = 1;
        var inFence = false;
        for (var lineStart = 0; lineStart < content.length();) {
            var lf = content.indexOf('\n', lineStart);
            var cr = content.indexOf('\r', lineStart);
            var newline = lf < 0 ? cr : cr < 0 ? lf : Math.min(lf, cr);
            var lineEnd = newline < 0 ? content.length() : newline + 1;
            if (newline >= 0 && content.charAt(newline) == '\r' && lineEnd < content.length()
                    && content.charAt(lineEnd) == '\n') lineEnd++;
            var line = content.substring(lineStart, lineEnd).replaceFirst("(?:\\r?\\n|\\r)$", "");
            var fence = MARKDOWN_FENCE.matcher(line);
            if (fence.matches()) {
                inFence = !inFence;
            } else if (!inFence) {
                var heading = MARKDOWN_HEADING.matcher(line);
                if (heading.matches()) {
                    nextOrder = appendStructuredBlock(content, bodyStart, lineStart, headingPath, maxCodePoints, nextOrder, result);
                    var level = heading.group(1).length();
                    while (headingPath.size() >= level) headingPath.removeLast();
                    var title = heading.group(2).replaceFirst("[ \\t]+#+\\s*$", "").trim();
                    headingPath.add(title);
                    bodyStart = lineEnd;
                }
            }
            lineStart = lineEnd;
        }
        appendStructuredBlock(content, bodyStart, content.length(), headingPath, maxCodePoints, nextOrder, result);
        return List.copyOf(result);
    }

    private int appendStructuredBlock(String content, int startChar, int endChar, List<String> headingPath,
                                      int maxCodePoints, int nextOrder, List<ChunkRange> output) {
        if (startChar >= endChar) return nextOrder;
        var block = content.substring(startChar, endChar);
        if (block.isBlank()) return nextOrder;
        var codePointOffset = content.codePointCount(0, startChar);
        for (var range : split(block, maxCodePoints)) {
            output.add(new ChunkRange(nextOrder++, codePointOffset + range.startCodePoint(),
                    codePointOffset + range.endCodePoint(), startChar + range.startChar(),
                    startChar + range.endChar(), List.copyOf(headingPath)));
        }
        return nextOrder;
    }

    private int lastParagraphBoundary(int[] codePoints, int start, int end) {
        for (var i = end; i > start; i--) {
            var lineBreaks = 0;
            var j = i;
            while (j > start && isLineBreak(codePoints[j - 1])) {
                if (codePoints[j - 1] == '\n') {
                    lineBreaks++;
                    j--;
                    if (j > start && codePoints[j - 1] == '\r') j--;
                } else {
                    lineBreaks++;
                    j--;
                }
            }
            if (lineBreaks >= 2) return i;
        }
        return -1;
    }

    private int lastLineBoundary(int[] codePoints, int start, int end) {
        for (var i = end; i > start; i--) {
            if (codePoints[i - 1] == '\n' || (codePoints[i - 1] == '\r' && (i == codePoints.length || codePoints[i] != '\n'))) return i;
        }
        return -1;
    }

    private boolean isLineBreak(int codePoint) { return codePoint == '\r' || codePoint == '\n'; }

    private boolean hasNonWhitespace(int[] codePoints, int start, int end) {
        for (var i = start; i < end; i++) if (!Character.isWhitespace(codePoints[i])) return true;
        return false;
    }

    private String jsonString(List<String> values) {
        try { return mapper.writeValueAsString(values); }
        catch (JsonProcessingException e) { throw new IllegalStateException("知识标题路径无法编码。", e); }
    }

    private List<String> stringList(String value) {
        try { return value == null ? List.of() : mapper.readValue(value, new TypeReference<List<String>>() { }); }
        catch (JsonProcessingException e) { throw new IllegalStateException("知识标题路径存储格式损坏。", e); }
    }

    private String embeddingText(ChunkWork chunk) {
        return chunk.headingPath().isEmpty() ? chunk.content()
                : String.join(" > ", chunk.headingPath()) + "\n\n" + chunk.content();
    }

    private record Idempotent(UUID id, String requestHash) { }
    private record VersionRequest(int assetVersion, String requestHash) { }
    private record ChunkSource(String sourceRef, int assetVersion, String content) { }
    private record ChunkRange(int order, int startCodePoint, int endCodePoint, int startChar, int endChar,
                              List<String> headingPath) { }
    private record BuildSource(int assetVersion, int totalChunks) { }
    private record BuildIdentity(KnowledgeIndexBuild build, String signature) { }
    private record BuildWork(UUID id, int assetVersion, String chunkingVersion, String provider, String model,
                             String modelRevision, int dimension, String configurationSignature, int maxBatchSize,
                             int maxInputTokens, int maxTextCodePoints, int retryGeneration,
                             int totalChunks, int nextChunkOrder) { }
    private record ChunkWork(UUID id, int order, String content, String contentHash, List<String> headingPath) { }
    private record SearchKey(UUID buildId, UUID chunkId) { }
    private static final class SearchMerge {
        private final KnowledgeSearchHit hit;
        private Integer vectorRank;
        private Integer lexicalRank;
        private double score;
        private SearchMerge(KnowledgeSearchHit hit) { this.hit = hit; }
    }
    private record DocumentState(long rowVersion, String status) { }
    private record VersionState(String contentHash, String status) { }
    private record ReadyBuild(UUID id, int assetVersion) { }
    private record PublicationPointer(int assetVersion, UUID buildId) { }
    private record ReplayTarget(String status, int attempts) { }
    private record KnowledgeReplayCommand(UUID commandId, UUID eventId, UUID actorId, String requestHash,
                                          String status, int attempts, Instant createdAt) { }
}
// 本文件负责把授权、校验、原文版本和幂等写入收敛在 knowledge 模块内。
