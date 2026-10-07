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
public class JdbcKnowledgeService implements KnowledgeService {
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "DELIVERED", "FAILED");
    private static final int MAX_CONTENT_BYTES = 100 * 1024;
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
    private final ObjectMapper mapper;
    private final Clock clock;
    private final EmbeddingGateway embeddings;
    private final AuditPort audit;

    @Autowired
    public JdbcKnowledgeService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                WorkspaceCatalog workspaceCatalog, IdentityDirectory identities,
                                ObjectMapper mapper, Clock clock, EmbeddingGateway embeddings, AuditPort audit) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.workspaceCatalog = workspaceCatalog;
        this.identities = identities;
        this.mapper = mapper;
        this.clock = clock;
        this.embeddings = embeddings;
        this.audit = audit;
    }

    /** 保留既有测试/嵌入式组装方的单空间构造入口； 分享 API 仅由 Spring 主装配启用。 */
    public JdbcKnowledgeService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                ObjectMapper mapper, Clock clock, EmbeddingGateway embeddings, AuditPort audit) {
        this(jdbc, workspaces, null, null, mapper, clock, embeddings, audit);
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

    // 对文档加行锁后 CAS 递增 rowVersion，防止两个编辑基于同一旧版本覆盖顺序。
    @Override
    @Transactional
    public KnowledgeDocument createVersion(CreateKnowledgeVersionCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null || command.documentId() == null
                || blank(command.content()) || command.content().getBytes(StandardCharsets.UTF_8).length > MAX_CONTENT_BYTES
                || blank(command.idempotencyKey()) || command.idempotencyKey().length() > 200 || hasControl(command.idempotencyKey()))
            throw EafException.invalid("新知识版本需要正文、范围和有效幂等键。");
        var access = workspaces.require(command.actor(), command.workspaceId(), "knowledge:write");
        var requestHash = Hashing.sha256("KNOWLEDGE_VERSION|" + command.content());
        var prior = findVersionByIdempotency(access, command.documentId(), command.idempotencyKey());
        if (prior != null) {
            if (!requestHash.equals(prior.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "版本幂等键已用于不同正文。");
            return getVersion(command.actor(), command.workspaceId(), command.documentId(), prior.assetVersion());
        }
        var document = lockDocumentForVersion(access, command.actor(), command.documentId()).orElseThrow(EafException::notFound);
        // 锁后再次读同键事实，收敛两个并发的相同版本请求。
        prior = findVersionByIdempotency(access, command.documentId(), command.idempotencyKey());
        if (prior != null) {
            if (!requestHash.equals(prior.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "版本幂等键已用于不同正文。");
            return getVersion(command.actor(), command.workspaceId(), command.documentId(), prior.assetVersion());
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
        return getVersion(command.actor(), command.workspaceId(), command.documentId(), assetVersion);
    }

    @Override
    public KnowledgeDocument get(ActorContext actor, UUID workspaceId, UUID documentId) {
        // 知识详情是资源枚举边界；没有 knowledge:read 时统一返回 404，避免新增审批角色改变历史脱敏语义。
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read")) {
            throw EafException.notFound();
        }
        return jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.title, d.source_ref, d.metadata::text, v.asset_version, v.content, v.content_hash, v.status, d.created_at, d.row_version from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = coalesce((select p.asset_version from knowledge.document_publication p where p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.status = 'ACTIVE'), (select max(draft.asset_version) from knowledge.document_version draft where draft.tenant_id = d.tenant_id and draft.workspace_id = d.workspace_id and draft.document_id = d.id and draft.status <> 'REVOKED')) join knowledge.document_permission p on p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE' where d.id = ? and d.tenant_id = ? and d.workspace_id = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeDocument>>) rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(), actor.actorId(), documentId, actor.tenantId(), workspaceId)
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
                + "and p.action = 'knowledge:read' and p.status = 'ACTIVE')";
        var total = jdbc.queryForObject("select count(*)" + where, Long.class,
                access.tenantId(), workspaceId, actor.actorId());
        var args = new java.util.ArrayList<Object>(List.of(access.tenantId(), workspaceId, actor.actorId()));
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
        return jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.title, d.source_ref, d.metadata::text, v.asset_version, v.content, v.content_hash, v.status, d.created_at, d.row_version from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id join knowledge.document_permission p on p.tenant_id = d.tenant_id and p.workspace_id = d.workspace_id and p.document_id = d.id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE' where d.id = ? and d.tenant_id = ? and d.workspace_id = ? and v.asset_version = ?",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeDocument>>) rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty(),
                actor.actorId(), documentId, actor.tenantId(), workspaceId, assetVersion).orElseThrow(EafException::notFound);
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
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        requireExternalEmbedding(actor, workspaceId, profile);
        var source = findBuildSource(actor, access, documentId, assetVersion, chunkingVersion).orElseThrow(EafException::notFound);
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
        var access = workspaces.require(actor, workspaceId, "knowledge:read");
        return jdbc.query("select b.* from knowledge.index_build b where b.id = ? and b.tenant_id = ? and b.workspace_id = ? and b.document_id = ? and exists (select 1 from knowledge.document_permission p where p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id and p.document_id = b.document_id and p.actor_id = ? and p.action = 'knowledge:read' and p.status = 'ACTIVE')",
                (org.springframework.jdbc.core.ResultSetExtractor<Optional<KnowledgeIndexBuild>>) rs -> rs.next() ? Optional.of(mapBuild(rs)) : Optional.empty(),
                buildId, access.tenantId(), access.workspaceId(), documentId, actor.actorId()).orElseThrow(EafException::notFound);
    }

    @Override
    public KnowledgeIndexBuild retryIndex(ActorContext actor, UUID workspaceId, UUID documentId, UUID buildId) {
        var access = workspaces.require(actor, workspaceId, "knowledge:write");
        var profile = embeddings.profile();
        requireStoredDimension(profile);
        requireExternalEmbedding(actor, workspaceId, profile);
        var build = getIndexBuild(actor, workspaceId, documentId, buildId);
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
        var count = jdbc.queryForObject("select count(*) from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? and v.status = 'PUBLISHED' join knowledge.chunk c on c.tenant_id = v.tenant_id and c.workspace_id = v.workspace_id and c.document_id = v.document_id and c.asset_version = v.asset_version and c.id = ? and c.content_hash = ? join knowledge.index_build b on b.id = ? and b.tenant_id = d.tenant_id and b.workspace_id = d.workspace_id and b.document_id = d.id and b.asset_version = v.asset_version and b.status = 'READY' where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and d.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')",
                Integer.class, documentVersion, chunkId, contentHash, buildId, access.tenantId(), access.workspaceId(), documentId, actor.actorId());
        return count != null && count == 1;
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
            var count = jdbc.queryForObject("select count(*) from knowledge.document d join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = ? and v.status = 'PUBLISHED' join knowledge.chunk c on c.tenant_id = v.tenant_id and c.workspace_id = v.workspace_id and c.document_id = v.document_id and c.asset_version = v.asset_version and c.id = ? and c.content_hash = ? join knowledge.index_build b on b.id = ? and b.tenant_id = d.tenant_id and b.workspace_id = d.workspace_id and b.document_id = d.id and b.asset_version = v.asset_version and b.status = 'READY' join knowledge.document_publication p on p.tenant_id = b.tenant_id and p.workspace_id = b.workspace_id and p.document_id = b.document_id and p.asset_version = b.asset_version and p.build_id = b.id and p.status = 'ACTIVE' where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')",
                    Integer.class, hit.documentVersion(), hit.chunkId(), hit.contentHash(), hit.buildId(), access.tenantId(), workspaceId,
                    hit.documentId(), actor.actorId());
            return count != null && count == 1 && scopedHit.shareId() == null && scopedHit.shareVersion() == null;
        }
        if (!"SHARE".equals(accessPath) || scopedHit.shareId() == null || scopedHit.shareVersion() == null) return false;
        // 正式上下文共享本身就是第二条授权路径；这里核验精确共享记录，不要求接收者另有文档直读权。
        var count = jdbc.queryForObject("select count(*) from knowledge.context_share s join knowledge.document d on d.tenant_id = s.tenant_id and d.workspace_id = s.workspace_id and d.id = s.document_id and d.status = 'PUBLISHED' join knowledge.document_version v on v.tenant_id = s.tenant_id and v.workspace_id = s.workspace_id and v.document_id = s.document_id and v.asset_version = s.document_version and v.status = 'PUBLISHED' join knowledge.document_publication p on p.tenant_id = s.tenant_id and p.workspace_id = s.workspace_id and p.document_id = s.document_id and p.asset_version = s.document_version and p.build_id = s.build_id and p.status = 'ACTIVE' join knowledge.index_build b on b.id = s.build_id and b.tenant_id = s.tenant_id and b.workspace_id = s.workspace_id and b.document_id = s.document_id and b.asset_version = s.document_version and b.status = 'READY' join knowledge.chunk c on c.tenant_id = b.tenant_id and c.workspace_id = b.workspace_id and c.document_id = b.document_id and c.asset_version = b.asset_version and c.id = ? and c.content_hash = ? where s.id = ? and s.row_version = ? and s.status = 'ACTIVE' and s.tenant_id = ? and s.workspace_id = ? and s.document_id = ? and s.recipient_id = ? and s.document_version = ? and s.build_id = ? and s.content_hash = v.content_hash",
                Integer.class, hit.chunkId(), hit.contentHash(), scopedHit.shareId(), scopedHit.shareVersion(), access.tenantId(), workspaceId,
                hit.documentId(), actor.actorId(), hit.documentVersion(), hit.buildId());
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
        var vectorHits = vectorCandidates(vector, access, actor, profile, "VECTOR".equals(mode) ? topK : 20);
        if ("VECTOR".equals(mode)) {
            var hits = new ArrayList<KnowledgeSearchHit>();
            for (var i = 0; i < vectorHits.size(); i++) hits.add(withRetrieval(vectorHits.get(i), List.of("VECTOR"), i + 1,
                    null, null, null));
            return new KnowledgeSearchResult(topK, List.copyOf(hits), mode);
        }
        var tokens = lexicalTokens(query);
        var tsQuery = tokens.isEmpty() ? null : String.join(" | ", tokens);
        var lexicalHits = tsQuery == null ? List.<KnowledgeSearchHit>of()
                : lexicalCandidates(tsQuery, vector, access, actor, profile);
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
                        + "where e.tenant_id = ? and e.workspace_id = ? and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and (s.id is not null or exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE')) "
                        + "order by distance, c.document_id, c.chunk_order, c.id limit ?",
                (rs, rowNum) -> new ScopedKnowledgeSearchHit(mapSearchHit(rs), rs.getString("access_path"),
                        rs.getObject("share_id", UUID.class), (Long) rs.getObject("share_version")),
                vector, actor.actorId(), actor.actorId(), actor.actorId(), actor.actorId(),
                access.tenantId(), access.workspaceId(), profile.provider(), profile.model(), profile.revision(),
                profile.dimension(), actor.actorId(), limit);
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
                                                      EmbeddingProfile profile, int limit) {
        // build、向量维度和配置签名必须完全相同；不同向量空间绝不参与同一排序。
        return jdbc.query("select c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.id chunk_id, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text, e.build_id, e.configuration_signature, e.distance_metric, e.embedding <=> ?::public.vector as distance from knowledge.embedding e join knowledge.chunk c on c.tenant_id = e.tenant_id and c.workspace_id = e.workspace_id and c.document_id = e.document_id and c.asset_version = e.asset_version and c.id = e.chunk_id join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = c.asset_version join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.asset_version = e.asset_version and p.build_id = e.build_id join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id and b.workspace_id = e.workspace_id and b.document_id = e.document_id and b.dimension = e.dimension and b.configuration_signature = e.configuration_signature and b.status = 'READY' where e.tenant_id = ? and e.workspace_id = ? and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and p.status = 'ACTIVE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') order by distance, c.document_id, c.chunk_order, c.id limit ?",
                (rs, rowNum) -> mapSearchHit(rs), vector, access.tenantId(), access.workspaceId(), profile.provider(),
                profile.model(), profile.revision(), profile.dimension(), actor.actorId(), limit);
    }

    private List<KnowledgeSearchHit> lexicalCandidates(String tsQuery, String vector, WorkspaceAccess access,
                                                       ActorContext actor, EmbeddingProfile profile) {
        return jdbc.query("select c.tenant_id, c.workspace_id, c.document_id, c.asset_version, c.id chunk_id, c.source_ref, c.chunking_version, c.chunk_order, c.start_offset, c.end_offset, c.offset_unit, c.content, c.content_hash, c.heading_path::text, e.build_id, e.configuration_signature, e.distance_metric, e.embedding <=> ?::public.vector as distance, ts_rank_cd(le.search_vector, ?::tsquery) as lexical_score from knowledge.lexical_entry le join knowledge.embedding e on e.tenant_id = le.tenant_id and e.workspace_id = le.workspace_id and e.document_id = le.document_id and e.asset_version = le.asset_version and e.build_id = le.build_id and e.chunk_id = le.chunk_id join knowledge.chunk c on c.tenant_id = e.tenant_id and c.workspace_id = e.workspace_id and c.document_id = e.document_id and c.asset_version = e.asset_version and c.id = e.chunk_id join knowledge.document d on d.tenant_id = c.tenant_id and d.workspace_id = c.workspace_id and d.id = c.document_id join knowledge.document_version v on v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id and v.document_id = d.id and v.asset_version = c.asset_version join knowledge.document_publication p on p.tenant_id = e.tenant_id and p.workspace_id = e.workspace_id and p.document_id = e.document_id and p.asset_version = e.asset_version and p.build_id = e.build_id join knowledge.index_build b on b.id = e.build_id and b.tenant_id = e.tenant_id and b.workspace_id = e.workspace_id and b.document_id = e.document_id and b.dimension = e.dimension and b.configuration_signature = e.configuration_signature and b.status = 'READY' where le.tenant_id = ? and le.workspace_id = ? and le.normalization_version = 'p9-lexical-1' and le.search_vector @@ ?::tsquery and b.provider = ? and b.model = ? and b.model_revision = ? and b.dimension = ? and b.distance_metric = 'COSINE' and p.status = 'ACTIVE' and d.status = 'PUBLISHED' and v.status = 'PUBLISHED' and exists (select 1 from knowledge.document_permission dp where dp.tenant_id = d.tenant_id and dp.workspace_id = d.workspace_id and dp.document_id = d.id and dp.actor_id = ? and dp.action = 'knowledge:read' and dp.status = 'ACTIVE') order by lexical_score desc, c.document_id, c.chunk_order, c.id limit 20",
                (rs, rowNum) -> mapSearchHit(rs), vector, tsQuery, access.tenantId(), access.workspaceId(), tsQuery,
                profile.provider(), profile.model(), profile.revision(), profile.dimension(), actor.actorId());
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
