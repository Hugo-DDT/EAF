package io.eaf.memory.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.CreateMemoryVersionCommand;
import io.eaf.memory.api.ExperienceCardService;
import io.eaf.memory.api.ExperienceCardService.ApplicableExperience;
import io.eaf.memory.api.ExperienceCardService.CardPage;
import io.eaf.memory.api.ExperienceCardService.CardReceipt;
import io.eaf.memory.api.ExperienceCardService.ExperienceCard;
import io.eaf.memory.api.ExperienceCardService.ExperienceCardRevision;
import io.eaf.memory.api.TeamExperienceService;
import io.eaf.memory.api.TeamExperienceService.SourceProof;
import io.eaf.memory.api.TeamExperienceService.ExperienceRef;
import io.eaf.memory.api.TeamExperienceService.TeamExperience;
import io.eaf.memory.api.TeamExperienceService.TeamExperiencePage;
import io.eaf.memory.api.TeamExperienceService.TeamExperienceRevision;
import io.eaf.memory.api.TeamExperienceService.TeamExperienceSelection;
import io.eaf.memory.api.MemoryDefinition;
import io.eaf.memory.api.MemoryCandidateReleaseCommand;
import io.eaf.memory.api.MemoryRelease;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.api.MemoryOutboxItem;
import io.eaf.memory.api.MemoryOutboxPage;
import io.eaf.memory.api.MemoryOutboxReplayReceipt;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.sql.Timestamp;
import java.text.Normalizer;
import java.time.Clock;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcMemoryService implements MemoryService, ExperienceCardService {
    private static final Set<String> OUTBOX_STATUSES = Set.of("PENDING", "DELIVERED", "FAILED");
    private static final int MAX_EVIDENCE_REFS = 16;
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final AuditPort audit;
    private final ObjectMapper json;
    private final Clock clock;

    public JdbcMemoryService(JdbcTemplate jdbc, WorkspaceAuthorization workspaces, AuditPort audit,
                             ObjectMapper json, Clock clock) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.audit = audit;
        this.json = json;
        this.clock = clock;
    }

    @Override
    @Transactional(readOnly = true)
    public CardPage list(ActorContext actor, UUID workspaceId, String applicability, String customerId,
                         String status, Instant cursorUpdatedAt, UUID cursorId, int limit) {
        requireExperienceAccess(actor, workspaceId, "memory:read");
        if (limit < 1 || limit > 100 || applicability != null && !Set.of("GENERAL", "CUSTOMER").contains(applicability)
                || status != null && !Set.of("ACTIVE", "DRAFT", "EXPIRED", "REVOKED").contains(status)
                || (cursorUpdatedAt == null) != (cursorId == null))
            throw EafException.invalid("经验卡筛选或 limit 无效。");
        if ("GENERAL".equals(applicability) && customerId != null
                || "CUSTOMER".equals(applicability) && (customerId == null || customerId.isBlank()))
            throw EafException.invalid("经验卡适用范围与 customerId 不匹配。");
        // 状态由当前 Memory 修订派生，先按稳定游标读取本人卡，再在应用层筛选状态。
        var sql = new StringBuilder("select memory_id from memory.experience_card where tenant_id = ? and workspace_id = ? and owner_id = ? and card_kind = 'PERSONAL'");
        var arguments = new java.util.ArrayList<Object>(java.util.Arrays.asList(actor.tenantId(), workspaceId, actor.actorId()));
        if (applicability != null) { sql.append(" and applicability = ?"); arguments.add(applicability); }
        if (customerId != null) { sql.append(" and customer_id = ?"); arguments.add(customerId); }
        if (cursorUpdatedAt != null) {
            sql.append(" and (updated_at, memory_id) < (?, ?)");
            arguments.add(Timestamp.from(cursorUpdatedAt));
            arguments.add(cursorId);
        }
        sql.append(" order by updated_at desc, memory_id desc");
        var rows = jdbc.query(sql.toString(), (rs, row) -> rs.getObject("memory_id", UUID.class), arguments.toArray());
        var cards = rows.stream().map(id -> readExperienceCard(actor, workspaceId, id))
                .filter(card -> status == null || status.equals(card.status())).toList();
        var hasNext = cards.size() > limit;
        var page = hasNext ? cards.subList(0, limit) : cards;
        var last = page.isEmpty() ? null : page.get(page.size() - 1);
        return new CardPage(page, hasNext ? last.updatedAt() : null, hasNext ? last.id() : null);
    }

    @Override
    @Transactional(readOnly = true)
    public ExperienceCard get(ActorContext actor, UUID workspaceId, UUID cardId) {
        requireExperienceAccess(actor, workspaceId, "memory:read");
        return readExperienceCard(actor, workspaceId, cardId);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ExperienceCardRevision> versions(ActorContext actor, UUID workspaceId, UUID cardId,
                                                  Integer beforeRevision, int limit) {
        var card = locklessCard(actor, workspaceId, cardId, "memory:read");
        if (limit < 1 || limit > 100 || beforeRevision != null && beforeRevision < 1)
            throw EafException.invalid("经验卡版本分页参数无效。");
        return jdbc.query("select r.revision, r.memory_version, r.title, r.source_task_id, r.source_feedback_id, r.draft_task_id, r.created_at, "
                        + "v.content, v.memory_type, v.status, v.expires_at from memory.experience_card_revision r "
                        + "join memory.version v on v.memory_id = r.memory_id and v.tenant_id = r.tenant_id and v.workspace_id = r.workspace_id and v.asset_version = r.memory_version "
                        + "where r.tenant_id = ? and r.workspace_id = ? and r.memory_id = ? and (? is null or r.revision < ?) "
                        + "order by r.revision desc limit ?",
                (rs, row) -> mapExperienceRevision(rs), actor.tenantId(), workspaceId, card.id(),
                beforeRevision, beforeRevision, limit);
    }

    @Override
    @Transactional
    public CardReceipt create(CreateExperienceCard command) {
        if (command == null) throw EafException.invalid("经验卡创建请求不完整。");
        var actor = command.actor();
        requireExperienceAccess(actor, command.workspaceId(), "memory:write");
        var title = requireText(command.title(), 80, "title");
        var content = requireText(command.content(), 800, "content");
        var type = experienceType(command.type());
        var applicability = command.applicability() == null ? "GENERAL" : command.applicability();
        if (!Set.of("GENERAL", "CUSTOMER").contains(applicability)
                || "CUSTOMER".equals(applicability) != (command.customerId() != null))
            throw EafException.invalid("experience applicability 与 customerId 不匹配。");
        var customerId = command.customerId() == null ? null : requireText(command.customerId(), 160, "customerId");
        validateExperienceSource(command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId());
        var expiresAt = command.expiresAt() == null ? Instant.now(clock).plus(java.time.Duration.ofDays(90)) : command.expiresAt();
        if (!expiresAt.isAfter(Instant.now(clock))) throw EafException.invalid("expiresAt 必须晚于当前时间。");
        // 幂等摘要记录调用方输入的有效期；默认 90 天只在首个写入时计算，重放时不能因时钟推进而改变摘要。
        var requestHash = Hashing.sha256(writeJson(new ExperienceCreateHash(title, content, type, applicability,
                customerId, command.expiresAt(), command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId())));
        var access = workspaces.require(actor, command.workspaceId(), "memory:write");
        var keyHash = experienceKeyHash(access.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var prior = experienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireExperienceReplay(actor, command.idempotencyKey(), requestHash, prior);

        var id = UUID.randomUUID();
        var logicalKey = "experience." + id;
        var sourceRef = command.sourceFeedbackId() == null ? "owner-entry:" + id : "feedback:" + command.sourceFeedbackId();
        var refs = experienceEvidenceRefs(id, command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId());
        var version = "1.0.0";
        var memoryCommand = new CreateMemoryVersionCommand(version, type, "PERSONAL", content, 0.5d, expiresAt,
                sourceRef, refs, "CUSTOMER".equals(applicability) ? "CUSTOMER" : null, customerId);
        var prepared = validate(actor, command.workspaceId(), memoryCommand);
        jdbc.update("insert into memory.definition(id, tenant_id, workspace_id, owner_id, logical_key) values (?, ?, ?, ?, ?)",
                id, access.tenantId(), access.workspaceId(), actor.actorId(), logicalKey);
        insertVersion(id, access.tenantId(), access.workspaceId(), memoryCommand, prepared);
        jdbc.update("insert into memory.experience_card_revision(memory_id, tenant_id, workspace_id, revision, memory_version, title, source_task_id, source_feedback_id, draft_task_id) values (?, ?, ?, 1, ?, ?, ?, ?, ?)",
                id, access.tenantId(), command.workspaceId(), version, title, command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId());
        jdbc.update("insert into memory.experience_card(memory_id, tenant_id, workspace_id, owner_id, applicability, customer_id, latest_revision) values (?, ?, ?, ?, ?, ?, 1)",
                id, access.tenantId(), command.workspaceId(), actor.actorId(), applicability, customerId);
        record(load(access.tenantId(), command.workspaceId(), id, version), actor, "EXPERIENCE_CARD_DRAFT_CREATED");
        var receipt = new CardReceipt(id, "CREATE", 1, version, 1, false);
        storeExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Override
    @Transactional
    public CardReceipt save(SaveExperienceCardVersion command) {
        if (command == null || command.cardId() == null || command.expectedVersion() < 1)
            throw EafException.invalid("经验卡保存请求不完整。");
        var actor = command.actor();
        var card = lockCard(actor, command.workspaceId(), command.cardId(), "memory:write");
        var title = requireText(command.title(), 80, "title");
        var content = requireText(command.content(), 800, "content");
        var type = experienceType(command.type());
        validateExperienceSource(command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId());
        var latest = experienceRevision(actor, command.workspaceId(), command.cardId(), card.latestRevision());
        var expiresAt = command.expiresAt() == null ? latest.expiresAt() : command.expiresAt();
        if (!expiresAt.isAfter(Instant.now(clock))) throw EafException.invalid("已过期卡片的新版本必须指定未来 expiresAt。");
        var requestHash = Hashing.sha256(writeJson(new ExperienceSaveHash(command.cardId(), command.expectedVersion(),
                title, content, type, expiresAt, command.sourceTaskId(), command.sourceFeedbackId(), command.draftTaskId())));
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireExperienceReplay(actor, command.idempotencyKey(), requestHash, prior);
        if (card.version() != command.expectedVersion()) throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "经验卡已被其他操作更新，请刷新后再保存。");
        var currentSourceTask = command.sourceTaskId() == null ? latest.sourceTaskId() : command.sourceTaskId();
        var currentFeedback = command.sourceFeedbackId() == null ? latest.sourceFeedbackId() : command.sourceFeedbackId();
        var currentDraft = command.draftTaskId() == null ? latest.draftTaskId() : command.draftTaskId();
        if (latest.title().equals(title) && latest.content().equals(content) && latest.type().equals(type)
                && latest.expiresAt().equals(expiresAt) && java.util.Objects.equals(latest.sourceTaskId(), currentSourceTask)
                && java.util.Objects.equals(latest.sourceFeedbackId(), currentFeedback)
                && java.util.Objects.equals(latest.draftTaskId(), currentDraft)) {
            var unchanged = new CardReceipt(card.id(), "SAVE", card.latestRevision(), latest.memoryVersion(), card.version(), false);
            storeExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, unchanged);
            return unchanged;
        }
        var revision = card.latestRevision() + 1;
        var memoryVersion = "1.0." + (revision - 1);
        var sourceRef = currentFeedback == null ? "owner-entry:" + card.id() : "feedback:" + currentFeedback;
        var memoryCommand = new CreateMemoryVersionCommand(memoryVersion, type, "PERSONAL", content, 0.5d, expiresAt,
                sourceRef, experienceEvidenceRefs(card.id(), currentSourceTask, currentFeedback, currentDraft),
                "CUSTOMER".equals(card.applicability()) ? "CUSTOMER" : null, card.customerId());
        var prepared = validate(actor, command.workspaceId(), memoryCommand);
        insertVersion(card.id(), actor.tenantId(), command.workspaceId(), memoryCommand, prepared);
        jdbc.update("insert into memory.experience_card_revision(memory_id, tenant_id, workspace_id, revision, memory_version, title, source_task_id, source_feedback_id, draft_task_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                card.id(), actor.tenantId(), command.workspaceId(), revision, memoryVersion, title,
                currentSourceTask, currentFeedback, currentDraft);
        if (jdbc.update("update memory.experience_card set latest_revision = ?, row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                revision, Timestamp.from(Instant.now(clock)), actor.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "经验卡已被其他操作更新。");
        var receipt = new CardReceipt(card.id(), "SAVE", revision, memoryVersion, card.version() + 1, false);
        storeExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        record(load(actor.tenantId(), command.workspaceId(), card.id(), memoryVersion), actor, "EXPERIENCE_CARD_DRAFT_SAVED");
        return receipt;
    }

    @Override
    @Transactional
    public CardReceipt publish(ExperienceCardService.PublishExperienceCard command) {
        if (command == null || command.cardId() == null || command.revision() < 1 || command.expectedVersion() < 1)
            throw EafException.invalid("经验卡启用请求不完整。");
        var actor = command.actor();
        var card = lockCard(actor, command.workspaceId(), command.cardId(), "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = Hashing.sha256("PUBLISH|" + command.cardId() + "|" + command.revision() + "|" + command.expectedVersion());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireExperienceReplay(actor, command.idempotencyKey(), requestHash, prior);
        if (card.version() != command.expectedVersion() || card.latestRevision() != command.revision())
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "只能启用当前最新草稿；请刷新经验卡后重试。");
        var revision = experienceRevision(actor, command.workspaceId(), card.id(), command.revision());
        if (!"DRAFT".equals(revision.status())) throw EafException.conflict("EXPERIENCE_NOT_DRAFT", "当前修订不是可启用草稿。");
        if (!revision.expiresAt().isAfter(Instant.now(clock))) throw EafException.conflict("MEMORY_EXPIRED", "已过期经验不能启用。");
        var access = workspaces.require(actor, command.workspaceId(), "memory:publish");
        var memory = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        if (jdbc.update("update memory.version set status = 'PUBLISHED', row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion(), memory.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "经验修订已被其他操作更新。");
        var published = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        publishMemoryFact(actor, access.tenantId(), command.workspaceId(), published, "EXPERIENCE_CARD_PUBLISHED");
        var now = Instant.now(clock);
        if (jdbc.update("update memory.experience_card set active_revision = ?, active_since = ?, row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                command.revision(), Timestamp.from(now), Timestamp.from(now), access.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "经验卡已被其他操作更新。");
        var receipt = new CardReceipt(card.id(), "PUBLISH", command.revision(), revision.memoryVersion(), card.version() + 1, false);
        storeExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Override
    @Transactional
    public CardReceipt revoke(RevokeExperienceCard command) {
        if (command == null || command.cardId() == null || command.expectedVersion() < 1 || command.expectedActiveRevision() < 1)
            throw EafException.invalid("经验卡撤回请求不完整。");
        var actor = command.actor();
        var card = lockCard(actor, command.workspaceId(), command.cardId(), "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = Hashing.sha256("REVOKE|" + command.cardId() + "|" + command.expectedVersion() + "|" + command.expectedActiveRevision());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireExperienceReplay(actor, command.idempotencyKey(), requestHash, prior);
        if (card.version() != command.expectedVersion() || !Integer.valueOf(command.expectedActiveRevision()).equals(card.activeRevision()))
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "当前启用修订已变化，不能撤回旧请求。");
        var revision = experienceRevision(actor, command.workspaceId(), card.id(), command.expectedActiveRevision());
        var access = workspaces.require(actor, command.workspaceId(), "memory:publish");
        var memory = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        if (!"PUBLISHED".equals(memory.status()) || jdbc.update("update memory.version set status = 'REVOKED', row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'PUBLISHED' and row_version = ?",
                access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion(), memory.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "经验修订已变化，不能撤回。");
        var revoked = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        revokeMemoryFact(actor, access.tenantId(), command.workspaceId(), revoked, "EXPERIENCE_CARD_REVOKED");
        var now = Instant.now(clock);
        if (jdbc.update("update memory.experience_card set active_revision = null, active_since = null, row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                Timestamp.from(now), access.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "经验卡已被其他操作更新。");
        var receipt = new CardReceipt(card.id(), "REVOKE", command.expectedActiveRevision(), revision.memoryVersion(), card.version() + 1, false);
        storeExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Override
    @Transactional(readOnly = true)
    public List<ApplicableExperience> findApplicable(ActorContext actor, UUID workspaceId, String customerId, int limit) {
        if (limit < 1 || limit > 100 || customerId != null && (customerId.isBlank() || customerId.length() > 160))
            throw EafException.invalid("经验上下文的条数或客户标识无效。");
        requireExperienceAccess(actor, workspaceId, "memory:read");
        var rows = jdbc.query("select c.memory_id, c.applicability, c.active_revision, r.memory_version, r.title "
                        + "from memory.experience_card c join memory.experience_card_revision r on r.memory_id = c.memory_id and r.tenant_id = c.tenant_id and r.workspace_id = c.workspace_id and r.revision = c.active_revision "
                        + "join memory.version v on v.memory_id = r.memory_id and v.tenant_id = r.tenant_id and v.workspace_id = r.workspace_id and v.asset_version = r.memory_version "
                        + "where c.tenant_id = ? and c.workspace_id = ? and c.owner_id = ? and c.card_kind = 'PERSONAL' and c.active_revision is not null "
                        + "and v.status = 'PUBLISHED' and v.expires_at > ? and (c.applicability = 'GENERAL' or c.customer_id = ?) "
                        + "order by case when c.applicability = 'CUSTOMER' then 0 else 1 end, c.active_since desc, c.memory_id limit ?",
                (rs, row) -> new ExperienceSelection(rs.getObject("memory_id", UUID.class), rs.getString("applicability"),
                        rs.getInt("active_revision"), rs.getString("memory_version"), rs.getString("title")),
                actor.tenantId(), workspaceId, actor.actorId(), Timestamp.from(Instant.now(clock)), customerId, limit);
        return rows.stream().map(row -> new ApplicableExperience(row.cardId(), row.revision(), row.memoryVersion(),
                row.title(), row.applicability(), requireUsable(actor, workspaceId, row.cardId(), row.memoryVersion()))).toList();
    }

    @Transactional(readOnly = true)
    public TeamExperiencePage listTeam(ActorContext actor, UUID workspaceId, String scenarioKey, boolean owned, String status,
            Instant cursorUpdatedAt, UUID cursorId, int limit) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        if (limit < 1 || limit > 50 || (cursorUpdatedAt == null) != (cursorId == null)
                || status != null && !Set.of("PUBLISHED", "DRAFT", "EXPIRED", "REVOKED").contains(status)
                || !owned && status != null && !"PUBLISHED".equals(status))
            throw EafException.invalid("团队经验分页参数无效。");
        if (scenarioKey != null) scenarioKey = teamScenario(scenarioKey);
        var sql = new StringBuilder("select c.memory_id from memory.experience_card c ");
        var cursorColumn = owned ? "c.updated_at" : "c.active_since";
        var args = new ArrayList<Object>();
        sql.append("where c.tenant_id = ? and c.workspace_id = ? and c.card_kind = 'TEAM' ");
        args.add(actor.tenantId()); args.add(workspaceId);
        if (owned) { sql.append("and c.owner_id = ? "); args.add(actor.actorId()); }
        else sql.append("and c.active_revision is not null and exists (select 1 from memory.experience_card_revision r "
                + "join memory.version v on v.memory_id = r.memory_id and v.tenant_id = r.tenant_id "
                + "and v.workspace_id = r.workspace_id and v.asset_version = r.memory_version "
                + "where r.memory_id = c.memory_id and r.revision = c.active_revision and v.status = 'PUBLISHED' and v.expires_at > now()) ");
        if (scenarioKey != null) { sql.append("and c.scenario_key = ? "); args.add(scenarioKey); }
        if (owned && status != null) {
            sql.append("and case when c.active_revision is not null and exists (select 1 from memory.experience_card_revision ar "
                    + "join memory.version av on av.memory_id = ar.memory_id and av.tenant_id = ar.tenant_id "
                    + "and av.workspace_id = ar.workspace_id and av.asset_version = ar.memory_version "
                    + "where ar.memory_id = c.memory_id and ar.revision = c.active_revision and av.status = 'PUBLISHED' and av.expires_at > now()) then 'PUBLISHED' "
                    + "when c.active_revision is not null then 'EXPIRED' "
                    + "when exists (select 1 from memory.experience_card_revision lr join memory.version lv "
                    + "on lv.memory_id = lr.memory_id and lv.tenant_id = lr.tenant_id and lv.workspace_id = lr.workspace_id "
                    + "and lv.asset_version = lr.memory_version where lr.memory_id = c.memory_id "
                    + "and lr.revision = c.latest_revision and lv.status = 'DRAFT') then 'DRAFT' else 'REVOKED' end = ? ");
            args.add(status);
        }
        if (cursorUpdatedAt != null) {
            sql.append("and (").append(cursorColumn).append(", c.memory_id) < (?, ?) ");
            args.add(Timestamp.from(cursorUpdatedAt)); args.add(cursorId);
        }
        sql.append("order by ").append(cursorColumn).append(" desc, c.memory_id desc");
        sql.append(" limit ?"); args.add(limit + 1);
        var ids = jdbc.query(sql.toString(), (rs, row) -> rs.getObject("memory_id", UUID.class), args.toArray());
        var cards = ids.stream().map(id -> readTeamExperience(actor, workspaceId, id))
                .filter(card -> status == null || status.equals(card.status())).toList();
        var hasNext = cards.size() > limit;
        var items = hasNext ? cards.subList(0, limit) : cards;
        var last = items.isEmpty() ? null : items.get(items.size() - 1);
        return new TeamExperiencePage(items, hasNext ? last.updatedAt() : null, hasNext ? last.id() : null);
    }

    @Transactional(readOnly = true)
    public TeamExperienceService.TeamExperienceDiscovery discoverTeam(ActorContext actor, UUID workspaceId,
            String scenarioKey, List<String> keywords, int limit) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        var scenario = teamScenario(scenarioKey);
        if (keywords == null || keywords.isEmpty() || keywords.size() > 8 || limit < 1 || limit > 20)
            throw EafException.invalid("团队经验发现仅接受 1—8 个关键词和 1—20 条结果。");
        var terms = new ArrayList<String>();
        for (var keyword : keywords) {
            if (keyword == null) throw EafException.invalid("发现关键词不能为空。");
            var normalized = Normalizer.normalize(keyword.trim(), Normalizer.Form.NFKC).trim().toLowerCase(Locale.ROOT);
            var length = normalized.codePointCount(0, normalized.length());
            if (length < 1 || length > 40) throw EafException.invalid("发现关键词长度须为 1—40 个字符。");
            if (!terms.contains(normalized)) terms.add(normalized);
        }
        var score = "sum((case when strpos(lower(r.title), kw.term) > 0 then 3 else 0 end) + "
                + "(case when strpos(lower(r.applies_when), kw.term) > 0 then 2 else 0 end) + "
                + "(case when strpos(lower(r.experience_content), kw.term) > 0 then 1 else 0 end))";
        var sql = new StringBuilder("with kw(term) as (values ");
        sql.append(String.join(", ", java.util.Collections.nCopies(terms.size(), "(?)")));
        sql.append(") select c.memory_id, ").append(score).append(" as score "
                + "from memory.experience_card c "
                + "join memory.experience_card_revision r on r.tenant_id = c.tenant_id and r.workspace_id = c.workspace_id "
                + "and r.memory_id = c.memory_id and r.revision = c.active_revision "
                + "join memory.version v on v.tenant_id = r.tenant_id and v.workspace_id = r.workspace_id "
                + "and v.memory_id = r.memory_id and v.asset_version = r.memory_version "
                + "cross join kw where c.tenant_id = ? and c.workspace_id = ? and c.card_kind = 'TEAM' "
                + "and c.scenario_key = ? and c.active_revision is not null and v.status = 'PUBLISHED' and v.expires_at > now() "
                + "group by c.memory_id having ").append(score).append(" > 0 order by score desc, c.memory_id asc limit ?");
        var args = new ArrayList<Object>(terms);
        args.add(actor.tenantId()); args.add(workspaceId); args.add(scenario); args.add(limit);
        var ranked = jdbc.query(sql.toString(), (rs, row) -> new RankedExperience(
                rs.getObject("memory_id", UUID.class), rs.getInt("score")), args.toArray());
        var items = ranked.stream().map(row -> {
            var card = getTeam(actor, workspaceId, row.cardId());
            var revision = card.active();
            if (revision == null) return null;
            var fields = new java.util.LinkedHashSet<String>();
            var matched = terms.stream().filter(term -> {
                var found = false;
                if (revision.title().toLowerCase(Locale.ROOT).contains(term)) { fields.add("title"); found = true; }
                if (revision.appliesWhen().toLowerCase(Locale.ROOT).contains(term)) { fields.add("appliesWhen"); found = true; }
                if (revision.content().toLowerCase(Locale.ROOT).contains(term)) { fields.add("content"); found = true; }
                return found;
            }).toList();
            if (matched.isEmpty()) return null;
            var source = revision.source() == null ? null : new TeamExperienceService.TeamExperienceSourceSummary(
                    revision.source().workItemId(), revision.source().outcome(), revision.source().completedAt());
            return new TeamExperienceService.DiscoveredTeamExperience(card.id(), revision.revision(),
                    revision.memoryVersion(), revision.contentHash(), revision.title(), revision.appliesWhen(), row.score(),
                    matched, List.copyOf(fields), preview(revision.content(), 200), revision.expiresAt(), source);
        }).filter(java.util.Objects::nonNull).toList();
        return new TeamExperienceService.TeamExperienceDiscovery("TEAM_KEYWORD_DISCOVERY_V1", items);
    }

    @Transactional(readOnly = true)
    public TeamExperience getTeam(ActorContext actor, UUID workspaceId, UUID cardId) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        return readTeamExperience(actor, workspaceId, cardId);
    }

    @Transactional(readOnly = true)
    public List<TeamExperienceRevision> versionsTeam(ActorContext actor, UUID workspaceId, UUID cardId,
            Integer beforeRevision, int limit) {
        var card = teamCard(actor, workspaceId, cardId, false);
        if (!actor.actorId().equals(card.ownerId())) throw EafException.notFound();
        if (limit < 1 || limit > 50 || beforeRevision != null && beforeRevision < 1)
            throw EafException.invalid("团队经验版本分页参数无效。");
        var revisions = jdbc.query("select revision from memory.experience_card_revision where tenant_id = ? and workspace_id = ? and memory_id = ? "
                        + "and (? is null or revision < ?) order by revision desc limit ?",
                (rs, row) -> rs.getInt("revision"), actor.tenantId(), workspaceId, cardId,
                beforeRevision, beforeRevision, limit);
        return revisions.stream().map(revision -> teamRevision(actor, workspaceId, cardId, revision, false)).toList();
    }

    @Transactional
    public TeamExperienceService.CardReceipt create(TeamExperienceService.CreateTeamExperience command) {
        if (command == null) throw EafException.invalid("团队经验创建请求不完整。");
        var actor = command.actor();
        requireTeamAccess(actor, command.workspaceId(), "memory:write");
        workspaces.require(actor, command.workspaceId(), "memory:scope:team");
        var scenario = teamScenario(command.scenarioKey());
        var title = requireText(command.title(), 80, "title");
        var appliesWhen = requireText(command.appliesWhen(), 300, "appliesWhen");
        var content = requireText(command.content(), 800, "content");
        var formatted = teamExperienceContent(appliesWhen, content);
        if (formatted.length() > 1_200) throw EafException.invalid("团队经验组合正文不能超过 1200 字符。");
        if (command.sourceWorkItemId() == null) throw EafException.invalid("sourceWorkItemId 必填。");
        var requestHash = teamCreateRequestHash(command);
        var access = workspaces.require(actor, command.workspaceId(), "memory:write");
        var keyHash = experienceKeyHash(access.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var prior = experienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        var expiresAt = command.expiresAt() == null ? Instant.now(clock).plus(java.time.Duration.ofDays(90)) : command.expiresAt();
        if (!expiresAt.isAfter(Instant.now(clock))) throw EafException.invalid("expiresAt 必须晚于当前时间。");
        var source = requireMatchingSource(command.sourceWorkItemId(), command.source());

        var id = UUID.randomUUID();
        var version = "1.0.0";
        var logicalKey = "team-experience." + id;
        var memoryCommand = new CreateMemoryVersionCommand(version, "PROCEDURAL", "TEAM", formatted, 0.5d,
                expiresAt, "work-item:" + source.workItemId(), teamEvidenceRefs(source), "SERVICE_REQUEST", scenario);
        var prepared = validate(actor, command.workspaceId(), memoryCommand);
        jdbc.update("insert into memory.definition(id, tenant_id, workspace_id, owner_id, logical_key) values (?, ?, ?, ?, ?)",
                id, access.tenantId(), command.workspaceId(), actor.actorId(), logicalKey);
        insertVersion(id, access.tenantId(), command.workspaceId(), memoryCommand, prepared);
        insertTeamRevision(id, access.tenantId(), command.workspaceId(), 1, version, title, appliesWhen, content, source);
        jdbc.update("insert into memory.experience_card(memory_id, tenant_id, workspace_id, owner_id, applicability, customer_id, latest_revision, card_kind, scenario_key) "
                        + "values (?, ?, ?, ?, 'SERVICE_REQUEST', null, 1, 'TEAM', ?)",
                id, access.tenantId(), command.workspaceId(), actor.actorId(), scenario);
        record(load(access.tenantId(), command.workspaceId(), id, version), actor, "TEAM_EXPERIENCE_DRAFT_CREATED");
        var receipt = new TeamExperienceService.CardReceipt(id, "CREATE", 1, version, 1, false);
        storeTeamExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Transactional
    public TeamExperienceService.CardReceipt save(TeamExperienceService.SaveTeamExperience command) {
        if (command == null || command.cardId() == null || command.expectedVersion() < 1)
            throw EafException.invalid("团队经验保存请求不完整。");
        var actor = command.actor();
        var card = lockTeamCard(actor, command.workspaceId(), command.cardId(), "memory:write");
        var title = requireText(command.title(), 80, "title");
        var appliesWhen = requireText(command.appliesWhen(), 300, "appliesWhen");
        var content = requireText(command.content(), 800, "content");
        var formatted = teamExperienceContent(appliesWhen, content);
        if (formatted.length() > 1_200) throw EafException.invalid("团队经验组合正文不能超过 1200 字符。");
        if (command.sourceWorkItemId() == null) throw EafException.invalid("sourceWorkItemId 必填。");
        var requestHash = teamSaveRequestHash(command);
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        if (card.version() != command.expectedVersion()) throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "团队经验已更新，请刷新后再保存。");
        var latest = teamRevision(actor, command.workspaceId(), card.id(), card.latestRevision(), false);
        var expiresAt = command.expiresAt() == null ? latest.expiresAt() : command.expiresAt();
        if (!expiresAt.isAfter(Instant.now(clock))) throw EafException.conflict("MEMORY_EXPIRED", "过期团队经验的新版本必须指定未来有效期。");
        var source = requireMatchingSource(command.sourceWorkItemId(), command.source());
        if (latest.title().equals(title) && latest.appliesWhen().equals(appliesWhen) && latest.content().equals(content)
                && latest.expiresAt().equals(expiresAt) && latest.source().equals(source)) {
            var unchanged = new TeamExperienceService.CardReceipt(card.id(), "SAVE", card.latestRevision(), latest.memoryVersion(), card.version(), false);
            storeTeamExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, unchanged);
            return unchanged;
        }
        var revision = card.latestRevision() + 1;
        var memoryVersion = "1.0." + (revision - 1);
        var memoryCommand = new CreateMemoryVersionCommand(memoryVersion, "PROCEDURAL", "TEAM", formatted, 0.5d,
                expiresAt, "work-item:" + source.workItemId(), teamEvidenceRefs(source), "SERVICE_REQUEST", card.scenarioKey());
        var prepared = validate(actor, command.workspaceId(), memoryCommand);
        insertVersion(card.id(), actor.tenantId(), command.workspaceId(), memoryCommand, prepared);
        insertTeamRevision(card.id(), actor.tenantId(), command.workspaceId(), revision, memoryVersion,
                title, appliesWhen, content, source);
        if (jdbc.update("update memory.experience_card set latest_revision = ?, row_version = row_version + 1, updated_at = ? "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                revision, Timestamp.from(Instant.now(clock)), actor.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "团队经验已被其他操作更新。");
        var receipt = new TeamExperienceService.CardReceipt(card.id(), "SAVE", revision, memoryVersion, card.version() + 1, false);
        storeTeamExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        record(load(actor.tenantId(), command.workspaceId(), card.id(), memoryVersion), actor, "TEAM_EXPERIENCE_DRAFT_SAVED");
        return receipt;
    }

    @Transactional
    public TeamExperienceService.CardReceipt publish(TeamExperienceService.PublishTeamExperience command) {
        if (command == null || command.cardId() == null || command.revision() < 1 || command.expectedVersion() < 1)
            throw EafException.invalid("团队经验发布请求不完整。");
        var actor = command.actor();
        var card = lockTeamCard(actor, command.workspaceId(), command.cardId(), "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = teamPublishRequestHash(command);
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        var source = validateSourceProof(command.source());
        if (card.version() != command.expectedVersion() || card.latestRevision() != command.revision())
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "只能发布当前最新团队经验草稿。");
        var revision = teamRevision(actor, command.workspaceId(), card.id(), command.revision(), false);
        if (!revision.source().equals(source)) throw EafException.conflict("TEAM_EXPERIENCE_SOURCE_CHANGED", "来源证明已变化，请重新保存草稿。");
        if (!"DRAFT".equals(revision.status()) || !revision.expiresAt().isAfter(Instant.now(clock)))
            throw EafException.conflict("EXPERIENCE_NOT_DRAFT", "当前修订不是有效草稿。");
        var access = workspaces.require(actor, command.workspaceId(), "memory:publish");
        var memory = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        if (jdbc.update("update memory.version set status = 'PUBLISHED', row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion(), memory.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "团队经验修订已被更新。");
        var published = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        publishMemoryFact(actor, access.tenantId(), command.workspaceId(), published, "TEAM_EXPERIENCE_PUBLISHED");
        var now = Instant.now(clock);
        if (jdbc.update("update memory.experience_card set active_revision = ?, active_since = ?, row_version = row_version + 1, updated_at = ? "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                command.revision(), Timestamp.from(now), Timestamp.from(now), access.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "团队经验已被其他操作更新。");
        var receipt = new TeamExperienceService.CardReceipt(card.id(), "PUBLISH", command.revision(), revision.memoryVersion(), card.version() + 1, false);
        storeTeamExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Transactional
    public TeamExperienceService.CardReceipt revoke(TeamExperienceService.RevokeTeamExperience command) {
        if (command == null || command.cardId() == null || command.expectedVersion() < 1 || command.expectedActiveRevision() < 1)
            throw EafException.invalid("团队经验撤回请求不完整。");
        var actor = command.actor();
        var card = lockTeamCard(actor, command.workspaceId(), command.cardId(), "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = Hashing.sha256("TEAM_REVOKE|" + command.cardId() + "|" + command.expectedVersion() + "|" + command.expectedActiveRevision());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        if (card.version() != command.expectedVersion() || !Integer.valueOf(command.expectedActiveRevision()).equals(card.activeRevision()))
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "当前发布修订已变化，不能撤回旧请求。");
        var revision = teamRevision(actor, command.workspaceId(), card.id(), command.expectedActiveRevision(), false);
        var access = workspaces.require(actor, command.workspaceId(), "memory:publish");
        var memory = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        if (!"PUBLISHED".equals(memory.status()) || jdbc.update("update memory.version set status = 'REVOKED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'PUBLISHED' and row_version = ?",
                access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion(), memory.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "团队经验修订已变化，不能撤回。");
        var revoked = load(access.tenantId(), command.workspaceId(), card.id(), revision.memoryVersion());
        revokeMemoryFact(actor, access.tenantId(), command.workspaceId(), revoked, "TEAM_EXPERIENCE_REVOKED");
        var now = Instant.now(clock);
        if (jdbc.update("update memory.experience_card set active_revision = null, active_since = null, row_version = row_version + 1, updated_at = ? "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and row_version = ?",
                Timestamp.from(now), access.tenantId(), command.workspaceId(), card.id(), command.expectedVersion()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "团队经验已被其他操作更新。");
        var receipt = new TeamExperienceService.CardReceipt(card.id(), "REVOKE", command.expectedActiveRevision(), revision.memoryVersion(), card.version() + 1, false);
        storeTeamExperienceCommand(access.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Transactional
    public TeamExperienceService.CardReceipt publishTeamCandidate(
            TeamExperienceService.PublishCandidateTeamExperience command) {
        if (command == null || command.candidateId() == null || command.candidateRevision() < 1
                || command.baseRevision() < 1 || command.expectedCardVersion() < 1)
            throw EafException.invalid("TEAM 候选发布绑定不完整。");
        var actor = command.actor();
        var card = lockTeamCard(actor, command.workspaceId(), command.cardId(), "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = Hashing.sha256(String.join("\u001f", "P23_TEAM_RELEASE", command.candidateId().toString(),
                Integer.toString(command.candidateRevision()), command.cardId().toString(),
                Integer.toString(command.baseRevision()), command.baseMemoryVersion(), command.contentHash()));
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        var title = requireText(command.title(), 80, "title");
        var appliesWhen = requireText(command.appliesWhen(), 300, "appliesWhen");
        var content = requireText(command.content(), 800, "content");
        var formatted = teamExperienceContent(appliesWhen, content);
        if (formatted.length() > 1_200 || command.contentHash() == null
                || !command.contentHash().equals(Hashing.sha256(String.join("\u001f", title, appliesWhen, content))))
            throw EafException.invalid("TEAM 候选正文或摘要无效。");
        var existing = jdbc.query("select memory_id, revision, memory_version, origin_card_version, origin_base_revision, "
                        + "origin_base_memory_version, origin_content_hash from memory.experience_card_revision where tenant_id = ? "
                        + "and workspace_id = ? and origin_candidate_id = ? and origin_candidate_revision = ?",
                rs -> rs.next() ? new TeamCandidateOrigin(rs.getObject("memory_id", UUID.class), rs.getInt("revision"),
                        rs.getString("memory_version"), rs.getLong("origin_card_version"),
                        rs.getInt("origin_base_revision"), rs.getString("origin_base_memory_version"),
                        rs.getString("origin_content_hash"), null) : null,
                actor.tenantId(), command.workspaceId(), command.candidateId(), command.candidateRevision());
        if (existing != null) {
            var released = teamRevision(actor, command.workspaceId(), existing.cardId(), existing.revision(), false);
            if (!existing.cardId().equals(command.cardId()) || existing.revision() != command.baseRevision() + 1
                    || existing.baseRevision() != command.baseRevision()
                    || !existing.baseMemoryVersion().equals(command.baseMemoryVersion())
                    || !existing.contentHash().equals(command.contentHash()) || !released.title().equals(title)
                    || !released.appliesWhen().equals(appliesWhen) || !released.content().equals(content))
                throw EafException.conflict("TEAM_CANDIDATE_RELEASE_CONFLICT", "候选来源已绑定不同的 TEAM 发布事实。");
            return new TeamExperienceService.CardReceipt(existing.cardId(), "PUBLISH_CANDIDATE", existing.revision(),
                    existing.memoryVersion(), existing.cardVersion(), true);
        }
        if (card.version() != command.expectedCardVersion() || card.latestRevision() != command.baseRevision()
                || !Integer.valueOf(command.baseRevision()).equals(card.activeRevision()))
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "TEAM 基线已变化或存在未发布修订。");
        var base = teamRevision(actor, command.workspaceId(), card.id(), command.baseRevision(), true);
        if (!base.memoryVersion().equals(command.baseMemoryVersion()) || !base.expiresAt().isAfter(Instant.now(clock)))
            throw EafException.conflict("TEAM_EXPERIENCE_BASE_CHANGED", "TEAM 候选基线已变化或到期。");
        var source = validateSourceProof(base.source());
        var revision = card.latestRevision() + 1;
        var memoryVersion = "1.0." + (revision - 1);
        var memoryCommand = new CreateMemoryVersionCommand(memoryVersion, "PROCEDURAL", "TEAM", formatted, 0.5d,
                base.expiresAt(), "work-item:" + source.workItemId(), teamEvidenceRefs(source),
                "SERVICE_REQUEST", card.scenarioKey());
        var prepared = validate(actor, command.workspaceId(), memoryCommand);
        insertVersion(card.id(), actor.tenantId(), command.workspaceId(), memoryCommand, prepared);
        insertTeamRevision(card.id(), actor.tenantId(), command.workspaceId(), revision, memoryVersion,
                title, appliesWhen, content, source);
        if (jdbc.update("update memory.experience_card_revision set origin_candidate_id = ?, origin_candidate_revision = ?, origin_card_version = ?, "
                        + "origin_base_revision = ?, origin_base_memory_version = ?, origin_content_hash = ? "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and revision = ? and origin_candidate_id is null",
                command.candidateId(), command.candidateRevision(), command.expectedCardVersion() + 1,
                command.baseRevision(), command.baseMemoryVersion(), command.contentHash(),
                actor.tenantId(), command.workspaceId(), card.id(), revision) != 1)
            throw EafException.conflict("TEAM_CANDIDATE_RELEASE_CONFLICT", "候选来源已绑定其他 TEAM 修订。");
        var draft = load(actor.tenantId(), command.workspaceId(), card.id(), memoryVersion);
        if (jdbc.update("update memory.version set status = 'PUBLISHED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? "
                        + "and status = 'DRAFT' and row_version = ?",
                actor.tenantId(), command.workspaceId(), card.id(), memoryVersion, draft.rowVersion()) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "TEAM 候选版本发布竞争失败。");
        var published = load(actor.tenantId(), command.workspaceId(), card.id(), memoryVersion);
        publishMemoryFact(actor, actor.tenantId(), command.workspaceId(), published, "TEAM_CANDIDATE_PUBLISHED");
        var now = Timestamp.from(Instant.now(clock));
        if (jdbc.update("update memory.experience_card set latest_revision = ?, active_revision = ?, active_since = ?, "
                        + "row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? "
                        + "and memory_id = ? and row_version = ? and latest_revision = ? and active_revision = ?",
                revision, revision, now, now, actor.tenantId(), command.workspaceId(), card.id(),
                command.expectedCardVersion(), command.baseRevision(), command.baseRevision()) != 1)
            throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "TEAM 基线在候选发布时发生变化。");
        record(published, actor, "TEAM_CANDIDATE_RELEASED");
        var receipt = new TeamExperienceService.CardReceipt(card.id(), "PUBLISH_CANDIDATE", revision,
                memoryVersion, card.version() + 1, false);
        storeTeamExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Transactional(readOnly = true)
    public Optional<TeamExperienceService.CardReceipt> findTeamCandidateRelease(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        var row = jdbc.query("select r.memory_id, r.revision, r.memory_version, r.origin_card_version, r.origin_base_revision, "
                        + "r.origin_base_memory_version, r.origin_content_hash, r.withdrawal_card_version from memory.experience_card_revision r "
                        + "where r.tenant_id = ? and r.workspace_id = ? "
                        + "and r.origin_candidate_id = ? and r.origin_candidate_revision = ?",
                rs -> rs.next() ? new TeamCandidateOrigin(rs.getObject("memory_id", UUID.class), rs.getInt("revision"),
                        rs.getString("memory_version"), rs.getLong("origin_card_version"), rs.getInt("origin_base_revision"),
                        rs.getString("origin_base_memory_version"), rs.getString("origin_content_hash"),
                        rs.getObject("withdrawal_card_version", Long.class)) : null,
                actor.tenantId(), workspaceId, candidateId, candidateRevision);
        if (row == null) return Optional.empty();
        return Optional.of(new TeamExperienceService.CardReceipt(row.cardId(), "PUBLISH_CANDIDATE",
                row.revision(), row.memoryVersion(), row.cardVersion(), true));
    }

    @Transactional
    public TeamExperienceService.CardReceipt revokeTeamCandidate(
            TeamExperienceService.RevokeCandidateTeamExperience command) {
        if (command == null || command.candidateId() == null || command.candidateRevision() < 1)
            throw EafException.invalid("TEAM 候选撤回绑定不完整。");
        var actor = command.actor();
        requireTeamAccess(actor, command.workspaceId(), "memory:publish");
        var origin = jdbc.query("select memory_id from memory.experience_card_revision where tenant_id = ? and workspace_id = ? "
                        + "and origin_candidate_id = ? and origin_candidate_revision = ?",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, actor.tenantId(), command.workspaceId(),
                command.candidateId(), command.candidateRevision());
        if (origin == null) throw EafException.notFound();
        var card = lockTeamCard(actor, command.workspaceId(), origin, "memory:publish");
        var keyHash = experienceKeyHash(actor.tenantId(), command.workspaceId(), actor.actorId(), command.idempotencyKey());
        var requestHash = Hashing.sha256("P23_TEAM_WITHDRAW|" + command.candidateId() + "|" + command.candidateRevision());
        var prior = experienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash);
        if (prior != null) return requireTeamExperienceReplay(requestHash, prior);
        var row = jdbc.query("select revision, memory_version from memory.experience_card_revision where tenant_id = ? and workspace_id = ? "
                        + "and memory_id = ? and origin_candidate_id = ? and origin_candidate_revision = ?",
                rs -> rs.next() ? new TeamCandidateRevision(rs.getInt("revision"), rs.getString("memory_version")) : null,
                actor.tenantId(), command.workspaceId(), origin, command.candidateId(), command.candidateRevision());
        if (row == null) throw EafException.notFound();
        var memory = load(actor.tenantId(), command.workspaceId(), origin, row.memoryVersion());
        var cardVersion = card.version();
        if ("PUBLISHED".equals(memory.status())) {
            if (jdbc.update("update memory.version set status = 'REVOKED', row_version = row_version + 1 where tenant_id = ? "
                            + "and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'PUBLISHED' and row_version = ?",
                    actor.tenantId(), command.workspaceId(), origin, row.memoryVersion(), memory.rowVersion()) != 1)
                throw EafException.conflict("VERSION_CONFLICT", "TEAM 候选版本撤回竞争失败。");
            var revoked = load(actor.tenantId(), command.workspaceId(), origin, row.memoryVersion());
            revokeMemoryFact(actor, actor.tenantId(), command.workspaceId(), revoked, "TEAM_CANDIDATE_REVOKED");
            if (Integer.valueOf(row.revision()).equals(card.activeRevision())) {
                var now = Timestamp.from(Instant.now(clock));
                if (jdbc.update("update memory.experience_card set active_revision = null, active_since = null, "
                                + "row_version = row_version + 1, updated_at = ? where tenant_id = ? and workspace_id = ? "
                                + "and memory_id = ? and row_version = ? and active_revision = ?",
                        now, actor.tenantId(), command.workspaceId(), origin, card.version(), row.revision()) != 1)
                    throw EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "TEAM 活动版本在候选撤回时发生变化。");
                cardVersion++;
            }
        } else if (!"REVOKED".equals(memory.status())) {
            throw EafException.conflict("TEAM_CANDIDATE_NOT_PUBLISHED", "TEAM 候选版本当前不可撤回。");
        }
        if (jdbc.update("update memory.experience_card_revision set withdrawal_card_version = ? where tenant_id = ? "
                        + "and workspace_id = ? and memory_id = ? and revision = ? and origin_candidate_id = ? "
                        + "and origin_candidate_revision = ? and withdrawal_card_version is null",
                cardVersion, actor.tenantId(), command.workspaceId(), origin, row.revision(), command.candidateId(),
                command.candidateRevision()) != 1)
            throw EafException.conflict("TEAM_CANDIDATE_WITHDRAWAL_CONFLICT", "候选撤回回执已绑定其他操作。");
        var receipt = new TeamExperienceService.CardReceipt(origin, "REVOKE_CANDIDATE", row.revision(),
                row.memoryVersion(), cardVersion, false);
        storeTeamExperienceCommand(actor.tenantId(), command.workspaceId(), actor.actorId(), keyHash, requestHash, receipt);
        return receipt;
    }

    @Transactional(readOnly = true)
    public Optional<TeamExperienceService.CardReceipt> findTeamCandidateWithdrawal(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        var row = jdbc.query("select r.memory_id, r.revision, r.memory_version, r.origin_card_version, r.origin_base_revision, "
                        + "r.origin_base_memory_version, r.origin_content_hash, r.withdrawal_card_version, v.status "
                        + "from memory.experience_card_revision r join memory.version v "
                        + "on v.memory_id = r.memory_id and v.tenant_id = r.tenant_id and v.workspace_id = r.workspace_id "
                        + "and v.asset_version = r.memory_version where r.tenant_id = ? and r.workspace_id = ? "
                        + "and r.origin_candidate_id = ? and r.origin_candidate_revision = ? and r.withdrawal_card_version is not null",
                rs -> rs.next() && "REVOKED".equals(rs.getString("status"))
                        ? new TeamCandidateOrigin(rs.getObject("memory_id", UUID.class), rs.getInt("revision"),
                        rs.getString("memory_version"), rs.getLong("origin_card_version"),
                        rs.getInt("origin_base_revision"), rs.getString("origin_base_memory_version"),
                        rs.getString("origin_content_hash"), rs.getObject("withdrawal_card_version", Long.class)) : null,
                actor.tenantId(), workspaceId, candidateId, candidateRevision);
        if (row == null) return Optional.empty();
        return Optional.of(new TeamExperienceService.CardReceipt(row.cardId(), "REVOKE_CANDIDATE",
                row.revision(), row.memoryVersion(), row.cardVersion(), true));
    }

    @Transactional(readOnly = true)
    public TeamExperienceSelection requireCurrent(ActorContext actor, UUID workspaceId, String scenarioKey,
            UUID cardId, int revision) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        var scenario = teamScenario(scenarioKey);
        var card = teamCard(actor, workspaceId, cardId, false);
        if (!scenario.equals(card.scenarioKey()) || !Integer.valueOf(revision).equals(card.activeRevision()))
            throw EafException.conflict("TEAM_EXPERIENCE_STALE", "所选团队经验已撤回、纠正或不属于当前场景。");
        var selected = teamRevision(actor, workspaceId, cardId, revision, true);
        var current = requireUsable(actor, workspaceId, cardId, selected.memoryVersion());
        return new TeamExperienceSelection(cardId, revision, selected.memoryVersion(), selected.title(),
                selected.appliesWhen(), selected.content(), current);
    }

    public Optional<TeamExperienceService.CardReceipt> replayCreate(TeamExperienceService.CreateTeamExperience command) {
        if (command == null || command.sourceWorkItemId() == null) throw EafException.invalid("sourceWorkItemId 必填。");
        requireTeamAccess(command.actor(), command.workspaceId(), "memory:write");
        workspaces.require(command.actor(), command.workspaceId(), "memory:scope:team");
        var hash = teamCreateRequestHash(command);
        var key = experienceKeyHash(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), command.idempotencyKey());
        var prior = experienceCommand(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), key);
        return prior == null ? Optional.empty() : Optional.of(requireTeamExperienceReplay(hash, prior));
    }

    public Optional<TeamExperienceService.CardReceipt> replaySave(TeamExperienceService.SaveTeamExperience command) {
        if (command == null || command.cardId() == null || command.sourceWorkItemId() == null)
            throw EafException.invalid("团队经验保存请求不完整。");
        var card = lockTeamCard(command.actor(), command.workspaceId(), command.cardId(), "memory:write");
        var hash = teamSaveRequestHash(command);
        var key = experienceKeyHash(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), command.idempotencyKey());
        var prior = experienceCommand(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), key);
        return prior == null ? Optional.empty() : Optional.of(requireTeamExperienceReplay(hash, prior));
    }

    public Optional<TeamExperienceService.CardReceipt> replayPublish(TeamExperienceService.PublishTeamExperience command) {
        if (command == null || command.cardId() == null) throw EafException.invalid("团队经验发布请求不完整。");
        lockTeamCard(command.actor(), command.workspaceId(), command.cardId(), "memory:publish");
        var hash = teamPublishRequestHash(command);
        var key = experienceKeyHash(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), command.idempotencyKey());
        var prior = experienceCommand(command.actor().tenantId(), command.workspaceId(), command.actor().actorId(), key);
        return prior == null ? Optional.empty() : Optional.of(requireTeamExperienceReplay(hash, prior));
    }

    private void requireTeamAccess(ActorContext actor, UUID workspaceId, String permission) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("团队经验只允许直接 HUMAN 身份访问。");
        workspaces.require(actor, workspaceId, "memory:read");
        if (!"memory:read".equals(permission)) workspaces.require(actor, workspaceId, permission);
    }

    private String teamScenario(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9-]{0,63}"))
            throw EafException.invalid("scenarioKey 必须匹配 ^[a-z][a-z0-9-]{0,63}$。");
        return value;
    }

    private String preview(String value, int maxCodePoints) {
        var count = value.codePointCount(0, value.length());
        return count <= maxCodePoints ? value : value.substring(0, value.offsetByCodePoints(0, maxCodePoints));
    }

    private String teamExperienceContent(String appliesWhen, String content) {
        return "适用条件：" + appliesWhen + "\n建议：" + content;
    }

    private SourceProof validateSourceProof(SourceProof source) {
        if (source == null || source.workItemId() == null || source.instanceId() == null || source.workItemVersion() < 1
                || source.completedBy() == null || source.completedAt() == null
                || !Set.of("COMPLETED", "BLOCKED", "NEEDS_FOLLOWUP").contains(source.outcome())
                || source.resultHash() == null || !source.resultHash().matches("[0-9a-f]{64}")
                || !"USER".equals(source.sourceType()))
            throw EafException.invalid("人工来源证明不完整。");
        return source;
    }

    private SourceProof requireMatchingSource(UUID sourceWorkItemId, SourceProof source) {
        var proof = validateSourceProof(source);
        if (!sourceWorkItemId.equals(proof.workItemId()))
            throw EafException.conflict("TEAM_EXPERIENCE_SOURCE_MISMATCH", "来源证明与请求的工作项不匹配。");
        return proof;
    }

    private String teamCreateRequestHash(TeamExperienceService.CreateTeamExperience command) {
        return Hashing.sha256(writeJson(new TeamExperienceCreateHash(teamScenario(command.scenarioKey()),
                requireText(command.title(), 80, "title"), requireText(command.appliesWhen(), 300, "appliesWhen"),
                requireText(command.content(), 800, "content"), command.expiresAt(), command.sourceWorkItemId())));
    }

    private String teamSaveRequestHash(TeamExperienceService.SaveTeamExperience command) {
        return Hashing.sha256(writeJson(new TeamExperienceSaveHash(command.cardId(), command.expectedVersion(),
                requireText(command.title(), 80, "title"), requireText(command.appliesWhen(), 300, "appliesWhen"),
                requireText(command.content(), 800, "content"), command.expiresAt(), command.sourceWorkItemId())));
    }

    private String teamPublishRequestHash(TeamExperienceService.PublishTeamExperience command) {
        return Hashing.sha256("TEAM_PUBLISH|" + command.cardId() + "|" + command.revision() + "|" + command.expectedVersion());
    }

    private List<String> teamEvidenceRefs(SourceProof source) {
        return List.of("work-item:" + source.workItemId(), "workflow:" + source.instanceId(), "result-hash:" + source.resultHash());
    }

    private void insertTeamRevision(UUID cardId, UUID tenantId, UUID workspaceId, int revision, String memoryVersion,
            String title, String appliesWhen, String content, SourceProof source) {
        jdbc.update("insert into memory.experience_card_revision(memory_id, tenant_id, workspace_id, revision, memory_version, title, "
                        + "applies_when, experience_content, source_work_item_id, source_proof_json) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)",
                cardId, tenantId, workspaceId, revision, memoryVersion, title, appliesWhen, content,
                source.workItemId(), writeJson(source));
    }

    private TeamExperienceData teamCard(ActorContext actor, UUID workspaceId, UUID cardId, boolean lock) {
        requireTeamAccess(actor, workspaceId, "memory:read");
        var card = jdbc.query("select owner_id, scenario_key, latest_revision, active_revision, row_version, active_since, updated_at "
                        + "from memory.experience_card where tenant_id = ? and workspace_id = ? and memory_id = ? and card_kind = 'TEAM' "
                        + (lock ? "for update" : ""),
                rs -> rs.next() ? new TeamExperienceData(cardId, rs.getObject("owner_id", UUID.class), rs.getString("scenario_key"),
                        rs.getInt("latest_revision"), (Integer) rs.getObject("active_revision"), rs.getLong("row_version"),
                        rs.getTimestamp("active_since") == null ? null : rs.getTimestamp("active_since").toInstant(),
                        rs.getTimestamp("updated_at").toInstant()) : null, actor.tenantId(), workspaceId, cardId);
        if (card == null) throw EafException.notFound();
        return card;
    }

    private TeamExperienceData lockTeamCard(ActorContext actor, UUID workspaceId, UUID cardId, String permission) {
        requireTeamAccess(actor, workspaceId, permission);
        var card = teamCard(actor, workspaceId, cardId, true);
        if (!actor.actorId().equals(card.ownerId())) throw EafException.notFound();
        workspaces.require(actor, workspaceId, "memory:scope:team");
        return card;
    }

    private TeamExperience readTeamExperience(ActorContext actor, UUID workspaceId, UUID cardId) {
        var card = teamCard(actor, workspaceId, cardId, false);
        var owner = actor.actorId().equals(card.ownerId());
        if (!owner && card.activeRevision() == null) throw EafException.notFound();
        var latest = owner ? teamRevision(actor, workspaceId, card.id(), card.latestRevision(), false) : null;
        var active = card.activeRevision() == null ? null : teamRevision(actor, workspaceId, card.id(), card.activeRevision(), !owner);
        if (!owner && active == null) throw EafException.notFound();
        var status = active != null && "PUBLISHED".equals(active.status()) && active.expiresAt().isAfter(Instant.now(clock))
                ? "PUBLISHED" : active != null ? "EXPIRED" : latest != null && "DRAFT".equals(latest.status()) ? "DRAFT" : "REVOKED";
        // 非 Owner 投影只暴露 activeRevision；保存未发布修订不能通过 revision、rowVersion 或更新时间泄露。
        var visibleRevision = owner ? card.latestRevision() : card.activeRevision();
        return new TeamExperience(card.id(), card.ownerId(), card.scenarioKey(), visibleRevision, card.activeRevision(),
                owner ? card.version() : card.activeRevision(), status, owner ? card.updatedAt() : card.activeSince(), latest, active);
    }

    private TeamExperienceRevision teamRevision(ActorContext actor, UUID workspaceId, UUID cardId, int revision,
            boolean requireCurrent) {
        var row = jdbc.query("select r.memory_version, r.title, r.applies_when, r.experience_content, r.source_proof_json::text, "
                        + "r.created_at, r.origin_candidate_id, r.origin_candidate_revision "
                        + "from memory.experience_card_revision r where r.tenant_id = ? and r.workspace_id = ? and r.memory_id = ? and r.revision = ?",
                rs -> rs.next() ? new TeamRevisionData(rs.getString("memory_version"), rs.getString("title"),
                        rs.getString("applies_when"), rs.getString("experience_content"), rs.getString("source_proof_json"),
                        rs.getTimestamp("created_at").toInstant(), rs.getObject("origin_candidate_id", UUID.class),
                        (Integer) rs.getObject("origin_candidate_revision")) : null,
                actor.tenantId(), workspaceId, cardId, revision);
        if (row == null || row.proofJson() == null) throw EafException.notFound();
        var memory = requireCurrent ? requireUsable(actor, workspaceId, cardId, row.memoryVersion())
                : load(actor.tenantId(), workspaceId, cardId, row.memoryVersion());
        try {
            var proof = json.readValue(row.proofJson(), SourceProof.class);
            return new TeamExperienceRevision(revision, row.memoryVersion(), row.title(), row.appliesWhen(), row.content(),
                    memory.status(), memory.expiresAt(), proof, memory.contentHash(), row.createdAt(),
                    row.originCandidateId(), row.originCandidateRevision());
        } catch (JsonProcessingException invalid) { throw new IllegalStateException("来源证明无法读取。", invalid); }
    }

    private TeamExperienceService.CardReceipt requireTeamExperienceReplay(String requestHash, ExperienceCommand prior) {
        if (!requestHash.equals(prior.requestHash()))
            throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一经验卡请求键不能绑定不同操作或内容。");
        return new TeamExperienceService.CardReceipt(prior.cardId(), prior.action(), prior.revision(),
                prior.memoryVersion(), prior.cardVersion(), true);
    }

    private void storeTeamExperienceCommand(UUID tenantId, UUID workspaceId, UUID ownerId, String keyHash,
            String requestHash, TeamExperienceService.CardReceipt receipt) {
        jdbc.update("insert into memory.experience_command(command_id, tenant_id, workspace_id, owner_id, request_key_hash, request_hash, action, card_id, result_revision, result_memory_version, result_card_version) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                UUID.randomUUID(), tenantId, workspaceId, ownerId, keyHash, requestHash, receipt.action(), receipt.cardId(),
                receipt.revision(), receipt.memoryVersion(), receipt.cardVersion());
    }

    private record TeamExperienceData(UUID id, UUID ownerId, String scenarioKey, int latestRevision,
            Integer activeRevision, long version, Instant activeSince, Instant updatedAt) { }
    private record TeamRevisionData(String memoryVersion, String title, String appliesWhen, String content,
            String proofJson, Instant createdAt, UUID originCandidateId, Integer originCandidateRevision) { }
    private record TeamCandidateOrigin(UUID cardId, int revision, String memoryVersion, long cardVersion,
            int baseRevision, String baseMemoryVersion, String contentHash, Long withdrawalCardVersion) { }
    private record TeamCandidateRevision(int revision, String memoryVersion) { }
    private record RankedExperience(UUID cardId, int score) { }
    private record TeamExperienceCreateHash(String scenarioKey, String title, String appliesWhen, String content,
            Instant expiresAt, UUID sourceWorkItemId) { }
    private record TeamExperienceSaveHash(UUID cardId, long expectedVersion, String title, String appliesWhen,
            String content, Instant expiresAt, UUID sourceWorkItemId) { }

    @Override
    @Transactional
    public MemoryDefinition create(CreateMemoryCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null)
            throw EafException.invalid("Memory 创建请求不完整。");
        var access = workspaces.require(command.actor(), command.workspaceId(), "memory:write");
        var logicalKey = requireText(command.logicalKey(), 120, "logicalKey");
        if (!logicalKey.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*")) throw EafException.invalid("logicalKey 格式无效。");
        var version = new CreateMemoryVersionCommand(command.version(), command.type(), command.scope(), command.content(),
                command.confidence(), command.expiresAt(), command.sourceRef(), command.evidenceRefs(),
                command.businessEntityType(), command.businessEntityId());
        var prepared = validate(command.actor(), command.workspaceId(), version);
        var id = UUID.randomUUID();
        try {
            jdbc.update("insert into memory.definition(id, tenant_id, workspace_id, owner_id, logical_key) values (?, ?, ?, ?, ?)",
                    id, access.tenantId(), access.workspaceId(), command.actor().actorId(), logicalKey);
            insertVersion(id, access.tenantId(), access.workspaceId(), version, prepared);
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("VERSION_CONFLICT", "Memory 标识或版本已存在。");
        }
        var result = load(access.tenantId(), access.workspaceId(), id, version.version());
        record(result, command.actor(), "MEMORY_DRAFT_CREATED");
        return result;
    }

    @Override
    @Transactional
    public MemoryDefinition addVersion(ActorContext actor, UUID workspaceId, UUID memoryId,
                                       CreateMemoryVersionCommand command) {
        var access = workspaces.require(actor, workspaceId, "memory:write");
        var owner = ownerId(access.tenantId(), workspaceId, memoryId);
        if (owner == null) throw EafException.notFound();
        if (!owner.equals(actor.actorId())) throw EafException.forbidden("只有 Memory Owner 可以创建新版本。");
        requireNotExperienceCard(access.tenantId(), workspaceId, memoryId);
        var prepared = validate(actor, workspaceId, command);
        try {
            insertVersion(memoryId, access.tenantId(), workspaceId, command, prepared);
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("VERSION_CONFLICT", "Memory 版本已存在。");
        }
        var result = load(access.tenantId(), workspaceId, memoryId, command.version());
        record(result, actor, "MEMORY_VERSION_CREATED");
        return result;
    }

    @Override
    public List<MemoryDefinition> list(ActorContext actor, UUID workspaceId) {
        var access = workspaces.require(actor, workspaceId, "memory:read");
        var keys = jdbc.query("select d.id, v.asset_version from memory.definition d "
                        + "join memory.version v on v.memory_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and v.status = 'PUBLISHED' and v.expires_at > ? "
                        + "and (v.scope = 'TEAM' or d.owner_id = ?) and not exists (select 1 from memory.version newer "
                        + "where newer.tenant_id = v.tenant_id and newer.workspace_id = v.workspace_id "
                        + "and newer.memory_id = v.memory_id and newer.status = 'PUBLISHED' and newer.created_at > v.created_at) "
                        + "and not exists (select 1 from memory.experience_card ec where ec.memory_id = d.id) "
                        + "order by d.id limit 100",
                (rs, row) -> new MemoryKey(rs.getObject("id", UUID.class), rs.getString("asset_version")),
                access.tenantId(), workspaceId, Timestamp.from(Instant.now(clock)), actor.actorId());
        return keys.stream().map(key -> load(access.tenantId(), workspaceId, key.id(), key.version())).toList();
    }

    @Override
    public MemoryOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                                  Instant createdAfter, Instant cursorCreatedAt,
                                                  UUID cursorEventId, int pageSize) {
        var access = workspaces.require(actor, workspaceId, "memory:read");
        if (pageSize < 1 || pageSize > 100 || (cursorCreatedAt == null) != (cursorEventId == null)
                || statuses != null && !OUTBOX_STATUSES.containsAll(statuses))
            throw EafException.invalid("Memory Outbox 运维分页或状态过滤无效。");
        var where = new StringBuilder(" where r.tenant_id = ? and r.workspace_id = ? "
                + "and exists (select 1 from memory.definition d join memory.version v on v.memory_id = d.id "
                + "and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                + "where d.id = r.memory_id and d.tenant_id = r.tenant_id and d.workspace_id = r.workspace_id "
                + "and v.asset_version = r.memory_version and (d.owner_id = ? or "
                + "(v.scope = 'TEAM' and v.status = 'PUBLISHED' and v.expires_at > now())) "
                + "and not exists (select 1 from memory.version newer where newer.memory_id = v.memory_id "
                + "and newer.tenant_id = v.tenant_id and newer.workspace_id = v.workspace_id "
                + "and newer.status = 'PUBLISHED' and newer.created_at > v.created_at))");
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
        var from = " from memory.outbox o join memory.release r on r.release_id = o.event_id";
        var totalSize = jdbc.queryForObject("select count(*)" + from + where, Long.class, filters.toArray());
        var pageWhere = new StringBuilder(where);
        var pageArgs = new ArrayList<>(filters);
        if (cursorCreatedAt != null) {
            pageWhere.append(" and (o.created_at, o.event_id) < (?, ?)");
            pageArgs.add(Timestamp.from(cursorCreatedAt));
            pageArgs.add(cursorEventId);
        }
        pageArgs.add(pageSize + 1);
        // 只通过本域发布事实补齐记忆关联，不把记忆正文或事件 payload 返回给运维端。
        var selected = jdbc.query("select o.event_id, r.memory_id, r.memory_version, r.action, o.event_type, o.status, o.attempt_count, o.last_error, o.next_attempt_at, o.created_at"
                        + from + pageWhere + " order by o.created_at desc, o.event_id desc limit ?",
                (rs, row) -> new MemoryOutboxItem(rs.getObject("event_id", UUID.class),
                        rs.getObject("memory_id", UUID.class), rs.getString("memory_version"), rs.getString("action"),
                        rs.getString("event_type"), rs.getString("status"), rs.getInt("attempt_count"),
                        rs.getString("last_error"), rs.getTimestamp("next_attempt_at") == null ? null : rs.getTimestamp("next_attempt_at").toInstant(),
                        rs.getTimestamp("created_at").toInstant()), pageArgs.toArray());
        var hasNext = selected.size() > pageSize;
        var items = hasNext ? List.copyOf(selected.subList(0, pageSize)) : List.copyOf(selected);
        var last = hasNext ? items.get(items.size() - 1) : null;
        return new MemoryOutboxPage(items, totalSize == null ? 0 : totalSize,
                last == null ? null : last.createdAt(), last == null ? null : last.eventId());
    }

    @Override
    @Transactional
    public MemoryOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                                  String requestKey, String reason) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Memory Outbox 重投只允许本人 HUMAN 操作者执行。");
        if (eventId == null || requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("Memory Outbox 重投需要事件、请求键和 1 至 500 字符的原因。");
        var access = workspaces.require(actor, workspaceId, "memory:read");
        workspaces.require(actor, workspaceId, "memory:outbox:replay");
        var keyHash = Hashing.sha256(String.join("\u001f", access.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), eventId.toString(), normalizedReason));

        // Memory 先验证原版本的当前可见范围，再锁定自身 Outbox 行和重放命令。
        var target = jdbc.query("select o.status, o.attempt_count, r.memory_id, r.memory_version from memory.outbox o "
                        + "join memory.release r on r.release_id = o.event_id "
                        + "where o.event_id = ? and r.tenant_id = ? and r.workspace_id = ? for update of o",
                rs -> rs.next() ? new MemoryReplayTarget(rs.getString("status"), rs.getInt("attempt_count"),
                        rs.getObject("memory_id", UUID.class), rs.getString("memory_version")) : null,
                eventId, access.tenantId(), workspaceId);
        if (target == null) throw EafException.notFound();
        var memory = load(access.tenantId(), workspaceId, target.memoryId(), target.version());
        if (!canRead(actor, memory)) throw EafException.notFound();
        var prior = memoryReplayCommand(access.tenantId(), workspaceId, keyHash);
        if (prior != null) return requireMatchingMemoryReplay(actor, eventId, requestHash, prior);
        if (!"FAILED".equals(target.status()))
            throw EafException.conflict("OUTBOX_NOT_FAILED", "只有 FAILED 的 Memory Outbox 事件可重投。");

        var commandId = UUID.randomUUID();
        var now = Instant.now(clock);
        var inserted = jdbc.update("insert into memory.outbox_replay_command(command_id, tenant_id, workspace_id, event_id, "
                        + "actor_id, request_key_hash, request_hash, reason, result_status, attempts, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?) "
                        + "on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                commandId, access.tenantId(), workspaceId, eventId, actor.actorId(), keyHash, requestHash,
                normalizedReason, target.attempts(), Timestamp.from(now));
        if (inserted == 0) {
            prior = memoryReplayCommand(access.tenantId(), workspaceId, keyHash);
            if (prior == null) throw EafException.conflict("MEMORY_COMMAND_CONFLICT", "Memory Outbox 请求键已被并发命令占用。");
            return requireMatchingMemoryReplay(actor, eventId, requestHash, prior);
        }
        jdbc.update("update memory.outbox set status = 'PENDING', next_attempt_at = now() where event_id = ? and status = 'FAILED'", eventId);
        audit.append(new AuditFact("memory-outbox-replay:" + commandId, access.tenantId(), workspaceId,
                actor.actorId(), null, "MEMORY_OUTBOX_REPLAY_REQUESTED", "PENDING",
                "{\"eventId\":\"" + eventId + "\",\"attempts\":" + target.attempts()
                        + ",\"reasonHash\":\"" + Hashing.sha256(normalizedReason) + "\"}", null));
        return new MemoryOutboxReplayReceipt(commandId, eventId, "PENDING", target.attempts(), now, false);
    }

    private MemoryReplayCommand memoryReplayCommand(UUID tenantId, UUID workspaceId, String keyHash) {
        return jdbc.query("select command_id, event_id, actor_id, request_hash, result_status, attempts, created_at "
                        + "from memory.outbox_replay_command where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new MemoryReplayCommand(rs.getObject("command_id", UUID.class),
                        rs.getObject("event_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("request_hash"), rs.getString("result_status"), rs.getInt("attempts"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                tenantId, workspaceId, keyHash);
    }

    private MemoryOutboxReplayReceipt requireMatchingMemoryReplay(ActorContext actor, UUID eventId,
                                                                  String requestHash, MemoryReplayCommand prior) {
        if (!prior.eventId().equals(eventId) || !actor.actorId().equals(prior.actorId())
                || !requestHash.equals(prior.requestHash()))
            throw EafException.conflict("MEMORY_COMMAND_CONFLICT", "同一 Memory 重投请求键不能绑定不同事件、操作者或原因。");
        return new MemoryOutboxReplayReceipt(prior.commandId(), prior.eventId(), prior.status(),
                prior.attempts(), prior.createdAt(), true);
    }

    @Override
    public List<MemoryDefinition> findApplicable(ActorContext actor, UUID workspaceId, String businessEntityType,
                                                 String businessEntityId) {
        if ((businessEntityType == null) != (businessEntityId == null))
            throw EafException.invalid("业务实体类型与标识必须同时提供。");
        var access = workspaces.require(actor, workspaceId, "memory:read");
        var keys = jdbc.query("select d.id, v.asset_version from memory.definition d "
                        + "join memory.version v on v.memory_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and v.status = 'PUBLISHED' and v.expires_at > ? "
                        + "and (v.scope = 'TEAM' or d.owner_id = ?) "
                        + "and (v.business_entity_id is null or (v.business_entity_type = ? and v.business_entity_id = ?)) "
                        + "and not exists (select 1 from memory.version newer where newer.tenant_id = v.tenant_id "
                        + "and newer.workspace_id = v.workspace_id and newer.memory_id = v.memory_id "
                        + "and newer.status = 'PUBLISHED' and newer.created_at > v.created_at) "
                        + "and not exists (select 1 from memory.experience_card ec where ec.memory_id = d.id) "
                        + "order by case when v.business_entity_id is null then 1 else 0 end, d.logical_key, d.id limit 100",
                (rs, row) -> new MemoryKey(rs.getObject("id", UUID.class), rs.getString("asset_version")),
                access.tenantId(), workspaceId, Timestamp.from(Instant.now(clock)), actor.actorId(),
                businessEntityType, businessEntityId);
        return keys.stream().map(key -> load(access.tenantId(), workspaceId, key.id(), key.version())).toList();
    }

    @Override
    public MemoryDefinition get(ActorContext actor, UUID workspaceId, UUID memoryId, String version) {
        var access = workspaces.require(actor, workspaceId, "memory:read");
        var memory = load(access.tenantId(), workspaceId, memoryId, version);
        if (!canRead(actor, memory)) throw EafException.notFound();
        return memory;
    }

    @Override
    public MemoryDefinition requireUsable(ActorContext actor, UUID workspaceId, UUID memoryId, String version) {
        var memory = get(actor, workspaceId, memoryId, version);
        // 只有当前发布版本且尚未到期的 Memory 能进入后续 Context；旧版只保留历史读取。
        if (!"PUBLISHED".equals(memory.status()) || !memory.expiresAt().isAfter(Instant.now(clock)) || !isCurrent(memory))
            throw EafException.notFound();
        return memory;
    }

    @Override
    @Transactional
    public MemoryDefinition publish(ActorContext actor, UUID workspaceId, UUID memoryId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "memory:publish");
        var memory = load(access.tenantId(), workspaceId, memoryId, version);
        requireOwner(actor, memory);
        requireNotExperienceCard(access.tenantId(), workspaceId, memoryId);
        requireScope(actor, workspaceId, memory.scope());
        if (!memory.expiresAt().isAfter(Instant.now(clock))) throw EafException.conflict("MEMORY_EXPIRED", "已过期的 Memory 不能发布。");
        var changed = jdbc.update("update memory.version set status = 'PUBLISHED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'DRAFT' and row_version = ?",
                access.tenantId(), workspaceId, memoryId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "Memory 状态或版本已变化。");
        var published = load(access.tenantId(), workspaceId, memoryId, version);
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, content_hash) values (?, ?, ?, ?, ?, 'PUBLISHED', ?, ?)",
                releaseId, access.tenantId(), workspaceId, memoryId, version, actor.actorId(), published.contentHash());
        appendOutbox(releaseId, published, actor, "PUBLISHED", "OWNER", null, null, null);
        record(published, actor, "MEMORY_PUBLISHED");
        return published;
    }

    @Override
    @Transactional
    public MemoryRelease publishCandidate(MemoryCandidateReleaseCommand command) {
        if (command == null || command.actor() == null || command.workspaceId() == null
                || command.candidateId() == null || command.candidateRevision() < 1
                || command.proposedContent() == null || !command.proposedContent().isObject())
            throw EafException.invalid("候选 Memory 发布请求不完整。");
        var actor = command.actor();
        if (actor.type() != ActorType.HUMAN) throw EafException.forbidden("只有 HUMAN 可以发布学习 Memory。");
        var access = workspaces.require(actor, command.workspaceId(), "memory:publish");
        workspaces.require(actor, command.workspaceId(), "memory:write");
        if (command.memoryId() != null) requireNotExperienceCard(access.tenantId(), command.workspaceId(), command.memoryId());
        // 同一候选修订的并发重放在目标域串行化，数据库唯一索引仍是最终去重约束。
        jdbc.query("select pg_advisory_xact_lock(hashtext(?), ?)", rs -> { if (rs.next()) rs.getObject(1); return null; },
                command.candidateId().toString(), command.candidateRevision());
        var proposed = command.proposedContent();
        var allowed = java.util.Set.of("logicalKey", "type", "scope", "content", "confidence", "expiresAt",
                "sourceRef", "evidenceRefs", "businessEntityType", "businessEntityId");
        var keys = new java.util.HashSet<String>();
        proposed.fieldNames().forEachRemaining(keys::add);
        if (!allowed.containsAll(keys)) throw EafException.invalid("候选 Memory 含目标模块不支持的字段。");
        var logicalKey = requiredText(proposed, "logicalKey", 120);
        var versionName = UUID.nameUUIDFromBytes((command.candidateId() + ":" + command.candidateRevision())
                .getBytes(StandardCharsets.UTF_8)).toString();
        var version = new CreateMemoryVersionCommand(versionName, requiredText(proposed, "type", 20),
                requiredText(proposed, "scope", 20), requiredText(proposed, "content", 8_000),
                proposed.path("confidence").isNumber() ? proposed.path("confidence").asDouble() : null,
                parseInstant(proposed, "expiresAt"), requiredText(proposed, "sourceRef", 500),
                stringList(proposed.path("evidenceRefs")), optionalText(proposed, "businessEntityType", 80),
                optionalText(proposed, "businessEntityId", 160));
        var prepared = validate(actor, command.workspaceId(), version);
        var requestHash = Hashing.sha256(writeJson(new CandidateReleaseRequest(command.candidateId(),
                command.candidateRevision(), command.memoryId(), command.baseVersion(), logicalKey, proposed)));
        var existing = findCandidateRelease(access.tenantId(), command.workspaceId(), command.candidateId(),
                command.candidateRevision());
        if (existing != null) {
            if (!actor.actorId().equals(existing.actorId())) throw EafException.notFound();
            var storedHash = jdbc.queryForObject("select request_hash from memory.release where release_id = ?",
                    String.class, existing.releaseId());
            if (!requestHash.equals(storedHash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一候选修订已绑定不同 Memory 发布命令。");
            return existing;
        }

        UUID memoryId = command.memoryId();
        String baseVersion = command.baseVersion();
        if (memoryId == null) {
            if (baseVersion != null) throw EafException.invalid("新 Memory 候选不能携带 baseVersion。");
            memoryId = UUID.randomUUID();
            try {
                jdbc.update("insert into memory.definition(id, tenant_id, workspace_id, owner_id, logical_key) values (?, ?, ?, ?, ?)",
                        memoryId, access.tenantId(), access.workspaceId(), actor.actorId(), logicalKey);
            } catch (DuplicateKeyException duplicate) {
                throw EafException.conflict("BASE_VERSION_CONFLICT", "同一 Memory 逻辑键已有正式资产。");
            }
        } else {
            if (baseVersion == null || baseVersion.isBlank()) throw EafException.invalid("Memory 更新必须绑定 baseVersion。");
            var ownerAndKey = jdbc.query("select owner_id, logical_key from memory.definition where tenant_id = ? and workspace_id = ? and id = ? for update",
                    rs -> rs.next() ? new OwnerAndKey(rs.getObject("owner_id", UUID.class), rs.getString("logical_key")) : null,
                    access.tenantId(), access.workspaceId(), memoryId);
            if (ownerAndKey == null) throw EafException.notFound();
            if (!actor.actorId().equals(ownerAndKey.ownerId())) throw EafException.forbidden("只有 Memory Owner 可以发布候选修订。");
            var baseline = load(access.tenantId(), access.workspaceId(), memoryId, baseVersion);
            if (!"PUBLISHED".equals(baseline.status()) || !baseline.expiresAt().isAfter(Instant.now(clock))
                    || !isCurrent(baseline))
                throw EafException.conflict("BASE_VERSION_CONFLICT", "Memory 发布基线已变化、撤回或过期。");
            if (!logicalKey.equals(ownerAndKey.logicalKey()) || !baseline.scope().equals(prepared.scope())
                    || !java.util.Objects.equals(baseline.businessEntityType(), prepared.businessEntityType())
                    || !java.util.Objects.equals(baseline.businessEntityId(), prepared.businessEntityId()))
                throw EafException.conflict("CANDIDATE_SCOPE_CONFLICT", "Memory 更新不能改变逻辑键、范围或实体适用性。");
        }
        try {
            insertVersion(memoryId, access.tenantId(), access.workspaceId(), version, prepared, "LEARNING_RELEASE");
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("VERSION_CONFLICT", "候选 Memory 版本已被占用。");
        }
        if (jdbc.update("update memory.version set status = 'PUBLISHED', row_version = row_version + 1 where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'DRAFT' and row_version = 1",
                access.tenantId(), access.workspaceId(), memoryId, versionName) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "候选 Memory 版本未能发布。");
        var published = load(access.tenantId(), access.workspaceId(), memoryId, versionName);
        var releaseId = UUID.randomUUID();
        var now = Timestamp.from(Instant.now(clock));
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, occurred_at, release_origin, candidate_id, candidate_revision, base_version, content_hash, request_hash) values (?, ?, ?, ?, ?, 'PUBLISHED', ?, ?, 'LEARNING_CANDIDATE', ?, ?, ?, ?, ?)",
                releaseId, access.tenantId(), access.workspaceId(), memoryId, versionName, actor.actorId(), now,
                command.candidateId(), command.candidateRevision(), baseVersion, published.contentHash(), requestHash);
        appendOutbox(releaseId, published, actor, "PUBLISHED", "LEARNING_CANDIDATE", command.candidateId(),
                command.candidateRevision(), baseVersion);
        record(published, actor, "MEMORY_PUBLISHED_FROM_LEARNING");
        return new MemoryRelease(releaseId, memoryId, versionName, "PUBLISHED", "LEARNING_CANDIDATE",
                command.candidateId(), command.candidateRevision(), baseVersion, published.contentHash(), actor.actorId(), now.toInstant());
    }

    @Override
    @Transactional
    public MemoryDefinition revoke(ActorContext actor, UUID workspaceId, UUID memoryId, String version, long expectedVersion) {
        var access = workspaces.require(actor, workspaceId, "memory:publish");
        var memory = load(access.tenantId(), workspaceId, memoryId, version);
        requireOwner(actor, memory);
        requireNotExperienceCard(access.tenantId(), workspaceId, memoryId);
        var changed = jdbc.update("update memory.version set status = 'REVOKED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? and status = 'PUBLISHED' and row_version = ?",
                access.tenantId(), workspaceId, memoryId, version, expectedVersion);
        if (changed != 1) throw EafException.conflict("VERSION_CONFLICT", "只有当前发布版本可以撤回。");
        var revoked = load(access.tenantId(), workspaceId, memoryId, version);
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, content_hash) values (?, ?, ?, ?, ?, 'REVOKED', ?, ?)",
                releaseId, access.tenantId(), workspaceId, memoryId, version, actor.actorId(), revoked.contentHash());
        appendOutbox(releaseId, revoked, actor, "REVOKED", "OWNER", null, null, null);
        record(revoked, actor, "MEMORY_REVOKED");
        return revoked;
    }

    @Override
    @Transactional
    public MemoryRelease revokeCandidate(ActorContext actor, UUID workspaceId, UUID memoryId, String version,
                                         long expectedVersion, UUID candidateId, int candidateRevision) {
        if (actor == null || actor.type() != ActorType.HUMAN || memoryId == null || version == null
                || expectedVersion <= 0 || candidateId == null || candidateRevision < 1)
            throw EafException.invalid("学习 Memory 撤回请求不完整。");
        var access = workspaces.require(actor, workspaceId, "memory:publish");
        workspaces.require(actor, workspaceId, "memory:write");
        requireNotExperienceCard(access.tenantId(), workspaceId, memoryId);
        var requestHash = Hashing.sha256("LEARNING_REVOKE|" + candidateId + "|" + candidateRevision
                + "|" + memoryId + "|" + version + "|" + expectedVersion);
        var prior = findCandidateWithdrawal(access.tenantId(), workspaceId, candidateId, candidateRevision);
        if (prior != null) {
            var storedHash = jdbc.queryForObject("select request_hash from memory.release where release_id = ?",
                    String.class, prior.releaseId());
            if (!requestHash.equals(storedHash))
                throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一候选修订已绑定不同 Memory 撤回命令。");
            return prior;
        }
        var origin = jdbc.query("select release_id, base_version from memory.release where tenant_id = ? and workspace_id = ? "
                        + "and memory_id = ? and memory_version = ? and action = 'PUBLISHED' "
                        + "and release_origin = 'LEARNING_CANDIDATE' and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? new MemoryOrigin(rs.getObject("release_id", UUID.class), rs.getString("base_version")) : null,
                access.tenantId(), workspaceId, memoryId, version, candidateId, candidateRevision);
        if (origin == null) throw EafException.conflict("RELEASE_NOT_FOUND", "该 Memory 版本不属于指定候选修订。");
        var current = load(access.tenantId(), workspaceId, memoryId, version);
        requireOwner(actor, current);
        if (jdbc.update("update memory.version set status = 'REVOKED', row_version = row_version + 1 "
                        + "where tenant_id = ? and workspace_id = ? and memory_id = ? and asset_version = ? "
                        + "and status = 'PUBLISHED' and row_version = ?",
                access.tenantId(), workspaceId, memoryId, version, expectedVersion) != 1)
            throw EafException.conflict("VERSION_CONFLICT", "候选 Memory 版本已变化或已撤回。");
        var revoked = load(access.tenantId(), workspaceId, memoryId, version);
        var releaseId = UUID.randomUUID();
        var now = Timestamp.from(Instant.now(clock));
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, occurred_at, release_origin, candidate_id, candidate_revision, base_version, content_hash, request_hash) values (?, ?, ?, ?, ?, 'REVOKED', ?, ?, 'LEARNING_CANDIDATE', ?, ?, ?, ?, ?)",
                releaseId, access.tenantId(), workspaceId, memoryId, version, actor.actorId(), now,
                candidateId, candidateRevision, origin.baseVersion(), revoked.contentHash(), requestHash);
        appendOutbox(releaseId, revoked, actor, "REVOKED", "LEARNING_CANDIDATE", candidateId,
                candidateRevision, origin.baseVersion());
        record(revoked, actor, "MEMORY_REVOKED_FROM_LEARNING");
        return new MemoryRelease(releaseId, memoryId, version, "REVOKED", "LEARNING_CANDIDATE", candidateId,
                candidateRevision, origin.baseVersion(), revoked.contentHash(), actor.actorId(), now.toInstant());
    }

    @Override
    public MemoryRelease findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                             int candidateRevision) {
        if (candidateId == null || candidateRevision < 1) throw EafException.invalid("来源键无效。");
        var access = workspaces.require(actor, workspaceId, "memory:read");
        var release = jdbc.query("select r.release_id, r.memory_id, r.memory_version, r.action, r.release_origin, r.candidate_id, r.candidate_revision, r.base_version, r.content_hash, r.actor_id, r.occurred_at from memory.release r where r.tenant_id = ? and r.workspace_id = ? and r.release_origin = 'LEARNING_CANDIDATE' and r.candidate_id = ? and r.candidate_revision = ? order by r.occurred_at desc, r.release_id desc limit 1",
                rs -> rs.next() ? new MemoryRelease(rs.getObject("release_id", UUID.class), rs.getObject("memory_id", UUID.class),
                        rs.getString("memory_version"), rs.getString("action"), rs.getString("release_origin"),
                        rs.getObject("candidate_id", UUID.class), rs.getObject("candidate_revision", Integer.class),
                        rs.getString("base_version"), rs.getString("content_hash"), rs.getObject("actor_id", UUID.class),
                        rs.getTimestamp("occurred_at").toInstant()) : null,
                access.tenantId(), workspaceId, candidateId, candidateRevision);
        if (release == null) throw EafException.notFound();
        // 来源键只定位事实；正文是否可见仍由 Memory 自己的 Owner/Scope 规则判定。
        get(actor, workspaceId, release.memoryId(), release.memoryVersion());
        return release;
    }

    @Override
    public MemoryRelease findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                 int candidateRevision) {
        if (candidateId == null || candidateRevision < 1) throw EafException.invalid("来源键无效。");
        var access = workspaces.require(actor, workspaceId, "memory:read");
        var release = jdbc.query("select release_id, memory_id, memory_version, action, release_origin, candidate_id, "
                        + "candidate_revision, base_version, content_hash, actor_id, occurred_at from memory.release "
                        + "where tenant_id = ? and workspace_id = ? and action = 'REVOKED' "
                        + "and release_origin = 'LEARNING_CANDIDATE' and candidate_id = ? and candidate_revision = ? "
                        + "order by occurred_at desc, release_id desc limit 1",
                rs -> rs.next() ? mapRelease(rs) : null,
                access.tenantId(), workspaceId, candidateId, candidateRevision);
        if (release == null) throw EafException.notFound();
        get(actor, workspaceId, release.memoryId(), release.memoryVersion());
        return release;
    }

    private void publishMemoryFact(ActorContext actor, UUID tenantId, UUID workspaceId, MemoryDefinition memory,
                                   String auditAction) {
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, content_hash) values (?, ?, ?, ?, ?, 'PUBLISHED', ?, ?)",
                releaseId, tenantId, workspaceId, memory.id(), memory.version(), actor.actorId(), memory.contentHash());
        appendOutbox(releaseId, memory, actor, "PUBLISHED", "OWNER", null, null, null);
        record(memory, actor, auditAction);
    }

    private void revokeMemoryFact(ActorContext actor, UUID tenantId, UUID workspaceId, MemoryDefinition memory,
                                 String auditAction) {
        var releaseId = UUID.randomUUID();
        jdbc.update("insert into memory.release(release_id, tenant_id, workspace_id, memory_id, memory_version, action, actor_id, content_hash) values (?, ?, ?, ?, ?, 'REVOKED', ?, ?)",
                releaseId, tenantId, workspaceId, memory.id(), memory.version(), actor.actorId(), memory.contentHash());
        appendOutbox(releaseId, memory, actor, "REVOKED", "OWNER", null, null, null);
        record(memory, actor, auditAction);
    }

    private void requireExperienceAccess(ActorContext actor, UUID workspaceId, String permission) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("个人经验卡仅允许本人 HUMAN 操作者管理。");
        workspaces.require(actor, workspaceId, permission);
    }

    private ExperienceCardData lockCard(ActorContext actor, UUID workspaceId, UUID cardId, String permission) {
        requireExperienceAccess(actor, workspaceId, "memory:read");
        requireExperienceAccess(actor, workspaceId, permission);
        var card = jdbc.query("select owner_id, applicability, customer_id, latest_revision, active_revision, row_version, active_since, created_at, updated_at "
                        + "from memory.experience_card where tenant_id = ? and workspace_id = ? and memory_id = ? and card_kind = 'PERSONAL' for update",
                rs -> rs.next() ? mapExperienceCard(rs, cardId) : null, actor.tenantId(), workspaceId, cardId);
        if (card == null || !actor.actorId().equals(card.ownerId())) throw EafException.notFound();
        return card;
    }

    private ExperienceCardData locklessCard(ActorContext actor, UUID workspaceId, UUID cardId, String permission) {
        requireExperienceAccess(actor, workspaceId, "memory:read");
        requireExperienceAccess(actor, workspaceId, permission);
        var card = jdbc.query("select owner_id, applicability, customer_id, latest_revision, active_revision, row_version, active_since, created_at, updated_at "
                        + "from memory.experience_card where tenant_id = ? and workspace_id = ? and memory_id = ? and card_kind = 'PERSONAL'",
                rs -> rs.next() ? mapExperienceCard(rs, cardId) : null, actor.tenantId(), workspaceId, cardId);
        if (card == null || !actor.actorId().equals(card.ownerId())) throw EafException.notFound();
        return card;
    }

    private ExperienceCardData mapExperienceCard(java.sql.ResultSet rs, UUID cardId) throws java.sql.SQLException {
        return new ExperienceCardData(cardId, rs.getObject("owner_id", UUID.class), rs.getString("applicability"),
                rs.getString("customer_id"), rs.getInt("latest_revision"), (Integer) rs.getObject("active_revision"),
                rs.getLong("row_version"), rs.getTimestamp("active_since") == null ? null : rs.getTimestamp("active_since").toInstant(),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant());
    }

    private ExperienceCard readExperienceCard(ActorContext actor, UUID workspaceId, UUID cardId) {
        var card = locklessCard(actor, workspaceId, cardId, "memory:read");
        var latest = experienceRevision(actor, workspaceId, card.id(), card.latestRevision());
        var active = card.activeRevision() == null ? null : experienceRevision(actor, workspaceId, card.id(), card.activeRevision());
        String status;
        if (active != null && "PUBLISHED".equals(active.status()) && active.expiresAt().isAfter(Instant.now(clock))) status = "ACTIVE";
        else if ("DRAFT".equals(latest.status())) status = "DRAFT";
        else if (active != null && !active.expiresAt().isAfter(Instant.now(clock))) status = "EXPIRED";
        else status = "REVOKED";
        return new ExperienceCard(card.id(), card.ownerId(), card.applicability(), card.customerId(), card.latestRevision(),
                card.activeRevision(), card.version(), status, card.activeSince(), card.createdAt(), card.updatedAt(), latest, active);
    }

    private ExperienceCardRevision experienceRevision(ActorContext actor, UUID workspaceId, UUID cardId, int revision) {
        var found = jdbc.query("select r.revision, r.memory_version, r.title, r.source_task_id, r.source_feedback_id, r.draft_task_id, r.created_at, "
                        + "v.content, v.memory_type, v.status, v.expires_at from memory.experience_card_revision r "
                        + "join memory.version v on v.memory_id = r.memory_id and v.tenant_id = r.tenant_id and v.workspace_id = r.workspace_id and v.asset_version = r.memory_version "
                        + "where r.tenant_id = ? and r.workspace_id = ? and r.memory_id = ? and r.revision = ?",
                rs -> rs.next() ? mapExperienceRevision(rs) : null, actor.tenantId(), workspaceId, cardId, revision);
        if (found == null) throw EafException.notFound();
        return found;
    }

    private ExperienceCardRevision mapExperienceRevision(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new ExperienceCardRevision(rs.getInt("revision"), rs.getString("memory_version"), rs.getString("title"),
                rs.getString("content"), rs.getString("memory_type"), rs.getString("status"),
                rs.getTimestamp("expires_at").toInstant(), rs.getObject("source_task_id", UUID.class),
                rs.getObject("source_feedback_id", UUID.class), rs.getObject("draft_task_id", UUID.class),
                rs.getTimestamp("created_at").toInstant());
    }

    private void validateExperienceSource(UUID sourceTaskId, UUID sourceFeedbackId, UUID draftTaskId) {
        if ((sourceFeedbackId == null) != (sourceTaskId == null) || draftTaskId != null && sourceFeedbackId == null)
            throw EafException.invalid("反馈经验来源必须同时绑定 Task 和反馈，整理 Task 也必须绑定反馈。");
    }

    private List<String> experienceEvidenceRefs(UUID cardId, UUID sourceTaskId, UUID sourceFeedbackId, UUID draftTaskId) {
        var refs = new ArrayList<String>();
        refs.add("owner-entry:" + cardId);
        if (sourceTaskId != null) refs.add("task:" + sourceTaskId);
        if (sourceFeedbackId != null) refs.add("feedback:" + sourceFeedbackId);
        if (draftTaskId != null) refs.add("draft-task:" + draftTaskId);
        return List.copyOf(refs);
    }

    private String experienceType(String value) {
        var type = value == null ? "PROCEDURAL" : value;
        if (!Set.of("PROCEDURAL", "PREFERENCE").contains(type)) throw EafException.invalid("经验卡 type 仅支持 PROCEDURAL 或 PREFERENCE。");
        return type;
    }

    private String experienceKeyHash(UUID tenantId, UUID workspaceId, UUID ownerId, String key) {
        var value = requireText(key, 200, "Idempotency-Key");
        if (value.chars().anyMatch(ch -> ch < 0x20 || ch == 0x7f)) throw EafException.invalid("Idempotency-Key 含控制字符。");
        var hash = Hashing.sha256(tenantId + "\u001f" + workspaceId + "\u001f" + ownerId + "\u001f" + value);
        jdbc.query("select pg_advisory_xact_lock(hashtext(?), hashtext(?))", rs -> { if (rs.next()) rs.getObject(1); return null; },
                "experience-command", hash);
        return hash;
    }

    private ExperienceCommand experienceCommand(UUID tenantId, UUID workspaceId, UUID ownerId, String keyHash) {
        return jdbc.query("select action, card_id, result_revision, result_memory_version, result_card_version, request_hash "
                        + "from memory.experience_command where tenant_id = ? and workspace_id = ? and owner_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new ExperienceCommand(rs.getString("action"), rs.getObject("card_id", UUID.class),
                        rs.getInt("result_revision"), rs.getString("result_memory_version"), rs.getLong("result_card_version"),
                        rs.getString("request_hash")) : null, tenantId, workspaceId, ownerId, keyHash);
    }

    private CardReceipt requireExperienceReplay(ActorContext actor, String idempotencyKey, String requestHash,
                                                ExperienceCommand prior) {
        if (!requestHash.equals(prior.requestHash())) throw EafException.conflict("IDEMPOTENCY_CONFLICT", "同一经验卡请求键不能绑定不同操作或内容。");
        return new CardReceipt(prior.cardId(), prior.action(), prior.revision(), prior.memoryVersion(), prior.cardVersion(), true);
    }

    private void storeExperienceCommand(UUID tenantId, UUID workspaceId, UUID ownerId, String keyHash,
                                        String requestHash, CardReceipt receipt) {
        jdbc.update("insert into memory.experience_command(command_id, tenant_id, workspace_id, owner_id, request_key_hash, request_hash, action, card_id, result_revision, result_memory_version, result_card_version) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                UUID.randomUUID(), tenantId, workspaceId, ownerId, keyHash, requestHash, receipt.action(), receipt.cardId(),
                receipt.revision(), receipt.memoryVersion(), receipt.cardVersion());
    }

    private void requireNotExperienceCard(UUID tenantId, UUID workspaceId, UUID memoryId) {
        if (Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from memory.experience_card where tenant_id = ? and workspace_id = ? and memory_id = ?)",
                Boolean.class, tenantId, workspaceId, memoryId)))
            throw EafException.conflict("EXPERIENCE_CARD_MANAGED", "该 Memory 由个人经验卡管理，请使用 experience-cards API。");
    }

    private PreparedVersion validate(ActorContext actor, UUID workspaceId, CreateMemoryVersionCommand command) {
        if (command == null) throw EafException.invalid("Memory 版本不能为空。");
        var version = requireText(command.version(), 40, "version");
        if (!version.equals(command.version())) throw EafException.invalid("version 不能包含首尾空格。");
        var type = requireText(command.type(), 20, "type");
        if (!List.of("EPISODIC", "SEMANTIC", "PROCEDURAL", "PREFERENCE").contains(type)) throw EafException.invalid("Memory type 无效。");
        var scope = requireText(command.scope(), 20, "scope");
        if (!List.of("PERSONAL", "TEAM").contains(scope)) throw EafException.forbidden("当前 Workspace 只支持 PERSONAL 与 TEAM 范围。");
        requireScope(actor, workspaceId, scope);
        var content = requireText(command.content(), 8_000, "content");
        if (command.confidence() == null || !Double.isFinite(command.confidence()) || command.confidence() < 0 || command.confidence() > 1)
            throw EafException.invalid("confidence 必须是 0 到 1 之间的有限数值。");
        if (command.expiresAt() == null || !command.expiresAt().isAfter(Instant.now(clock)))
            throw EafException.invalid("expiresAt 必须晚于当前时间。");
        var sourceRef = requireText(command.sourceRef(), 500, "sourceRef");
        var evidence = validateEvidence(command.evidenceRefs());
        if ((command.businessEntityType() == null) != (command.businessEntityId() == null))
            throw EafException.invalid("业务实体类型与标识必须同时提供。");
        var entityType = command.businessEntityType() == null ? null : requireText(command.businessEntityType(), 80, "businessEntityType");
        var entityId = command.businessEntityId() == null ? null : requireText(command.businessEntityId(), 160, "businessEntityId");
        return new PreparedVersion(type, scope, content, command.confidence(), command.expiresAt(), sourceRef, evidence,
                entityType, entityId);
    }

    private void requireScope(ActorContext actor, UUID workspaceId, String scope) {
        // 目前组织层级只覆盖当前 Workspace；团队范围必须另有显式范围授权。
        if ("TEAM".equals(scope)) workspaces.require(actor, workspaceId, "memory:scope:team");
    }

    private List<String> validateEvidence(List<String> evidenceRefs) {
        if (evidenceRefs == null || evidenceRefs.isEmpty() || evidenceRefs.size() > MAX_EVIDENCE_REFS)
            throw EafException.invalid("evidenceRefs 必须包含 1 到 16 条来源引用。");
        var unique = new HashSet<String>();
        var result = new ArrayList<String>();
        for (var ref : evidenceRefs) {
            var value = requireText(ref, 500, "evidenceRef");
            if (!unique.add(value)) throw EafException.invalid("evidenceRefs 不能重复。");
            result.add(value);
        }
        return List.copyOf(result);
    }

    private void insertVersion(UUID memoryId, UUID tenantId, UUID workspaceId, CreateMemoryVersionCommand command,
                               PreparedVersion prepared) {
        insertVersion(memoryId, tenantId, workspaceId, command, prepared, "OWNER_ATTESTATION");
    }

    private void insertVersion(UUID memoryId, UUID tenantId, UUID workspaceId, CreateMemoryVersionCommand command,
                               PreparedVersion prepared, String sourceType) {
        jdbc.update("insert into memory.version(memory_id, tenant_id, workspace_id, asset_version, memory_type, scope, content, confidence, expires_at, source_type, source_ref, evidence_refs, business_entity_type, business_entity_id, status) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'DRAFT')",
                memoryId, tenantId, workspaceId, command.version(), prepared.type(), prepared.scope(), prepared.content(),
                prepared.confidence(), Timestamp.from(prepared.expiresAt()), sourceType, prepared.sourceRef(), writeEvidence(prepared.evidenceRefs()),
                prepared.businessEntityType(), prepared.businessEntityId());
    }

    private MemoryDefinition load(UUID tenantId, UUID workspaceId, UUID memoryId, String version) {
        var memory = jdbc.query("select d.id, d.tenant_id, d.workspace_id, d.owner_id, d.logical_key, v.asset_version, "
                        + "v.memory_type, v.scope, v.business_entity_type, v.business_entity_id, v.content, v.confidence, v.expires_at, v.source_type, v.source_ref, "
                        + "v.evidence_refs::text evidence_refs, v.status, v.row_version "
                        + "from memory.definition d join memory.version v on v.memory_id = d.id and v.tenant_id = d.tenant_id and v.workspace_id = d.workspace_id "
                        + "where d.tenant_id = ? and d.workspace_id = ? and d.id = ? and v.asset_version = ?",
                rs -> rs.next() ? map(rs) : null, tenantId, workspaceId, memoryId, version);
        if (memory == null) throw EafException.notFound();
        return memory;
    }

    private MemoryDefinition map(java.sql.ResultSet rs) throws java.sql.SQLException {
        var id = rs.getObject("id", UUID.class);
        var tenant = rs.getObject("tenant_id", UUID.class);
        var workspace = rs.getObject("workspace_id", UUID.class);
        var owner = rs.getObject("owner_id", UUID.class);
        var logicalKey = rs.getString("logical_key");
        var version = rs.getString("asset_version");
        var type = rs.getString("memory_type");
        var scope = rs.getString("scope");
        var businessEntityType = rs.getString("business_entity_type");
        var businessEntityId = rs.getString("business_entity_id");
        var content = rs.getString("content");
        var confidence = rs.getDouble("confidence");
        var expiresAt = rs.getTimestamp("expires_at").toInstant();
        var sourceType = rs.getString("source_type");
        var sourceRef = rs.getString("source_ref");
        var evidenceRefs = readEvidence(rs.getString("evidence_refs"));
        var status = rs.getString("status");
        var rowVersion = rs.getLong("row_version");
        var hash = contentHash(new MemoryContent(id, tenant, workspace, owner, logicalKey, version, type, scope,
                businessEntityType, businessEntityId,
                content, confidence, expiresAt, sourceType, sourceRef, evidenceRefs));
        return new MemoryDefinition(id, tenant, workspace, owner, logicalKey, version, type, scope,
                businessEntityType, businessEntityId, content,
                confidence, expiresAt, sourceType, sourceRef, evidenceRefs, status, rowVersion, hash);
    }

    private boolean canRead(ActorContext actor, MemoryDefinition memory) {
        // PERSONAL 永不因拥有 Workspace 读取权而共享，TEAM 只对同一 Workspace 的已发布有效版本可见。
        if ("PERSONAL".equals(memory.scope())) return memory.ownerId().equals(actor.actorId());
        return memory.ownerId().equals(actor.actorId()) || ("PUBLISHED".equals(memory.status())
                && memory.expiresAt().isAfter(Instant.now(clock)));
    }

    private UUID ownerId(UUID tenantId, UUID workspaceId, UUID memoryId) {
        return jdbc.query("select owner_id from memory.definition where id = ? and tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? rs.getObject("owner_id", UUID.class) : null, memoryId, tenantId, workspaceId);
    }

    private boolean isCurrent(MemoryDefinition memory) {
        var activeCardVersion = jdbc.query("select r.memory_version from memory.experience_card c "
                        + "join memory.experience_card_revision r on r.memory_id = c.memory_id and r.tenant_id = c.tenant_id "
                        + "and r.workspace_id = c.workspace_id and r.revision = c.active_revision "
                        + "where c.tenant_id = ? and c.workspace_id = ? and c.memory_id = ?",
                rs -> rs.next() ? rs.getString(1) : null, memory.tenantId(), memory.workspaceId(), memory.id());
        if (activeCardVersion != null) return activeCardVersion.equals(memory.version());
        if (Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from memory.experience_card where tenant_id = ? and workspace_id = ? and memory_id = ?)",
                Boolean.class, memory.tenantId(), memory.workspaceId(), memory.id()))) return false;
        // 新版发布后不回落到旧版本；只有撤回新版或新建有效版本才改变当前解析结果。
        var newer = jdbc.queryForObject("select count(*) from memory.version newer join memory.version current "
                        + "on current.tenant_id = newer.tenant_id and current.workspace_id = newer.workspace_id "
                        + "and current.memory_id = newer.memory_id where current.tenant_id = ? and current.workspace_id = ? "
                        + "and current.memory_id = ? and current.asset_version = ? and newer.status = 'PUBLISHED' "
                        + "and newer.created_at > current.created_at",
                Integer.class, memory.tenantId(), memory.workspaceId(), memory.id(), memory.version());
        return newer != null && newer == 0;
    }

    private void requireOwner(ActorContext actor, MemoryDefinition memory) {
        if (!memory.ownerId().equals(actor.actorId())) throw EafException.forbidden("只有 Memory Owner 可以执行此操作。");
    }

    private void record(MemoryDefinition memory, ActorContext actor, String action) {
        audit.append(new AuditFact("memory:" + memory.id() + ":" + memory.version() + ":" + action,
                memory.tenantId(), memory.workspaceId(), actor.actorId(), null, action, "ACCEPTED",
                "{\"contentHash\":\"" + memory.contentHash() + "\"}", "memory:" + memory.id()));
    }

    private void appendOutbox(UUID releaseId, MemoryDefinition memory, ActorContext actor, String action,
                              String releaseOrigin, UUID candidateId, Integer candidateRevision, String baseVersion) {
        try {
            var payload = new MemoryOutboxPayload(releaseId, memory.tenantId(), memory.workspaceId(), memory.id(),
                    memory.version(), action, releaseOrigin, candidateId, candidateRevision, baseVersion,
                    memory.contentHash(), actor.actorId());
            var eventType = "PUBLISHED".equals(action)
                    ? "eaf.memory.version-published.v1" : "eaf.memory.version-revoked.v1";
            // 发布事实与通知意图在同一事务提交，消费者崩溃后仍可查询来源结果。
            jdbc.update("insert into memory.outbox(event_id, event_type, payload) values (?, ?, ?::jsonb)",
                    releaseId, eventType, json.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Memory 发布事件无法序列化。", e);
        }
    }

    private String contentHash(MemoryContent content) {
        try { return Hashing.sha256(json.writeValueAsString(content)); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Memory 内容摘要无法生成。", e); }
    }

    private String writeEvidence(List<String> evidenceRefs) {
        try { return json.writeValueAsString(evidenceRefs); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Memory 来源无法序列化。", e); }
    }

    private String writeJson(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("候选 Memory 发布请求无法序列化。", e); }
    }

    private String requiredText(JsonNode node, String field, int maxLength) {
        if (!node.path(field).isTextual()) throw EafException.invalid(field + " 必须是文本。");
        return requireText(node.path(field).asText(), maxLength, field);
    }

    private String optionalText(JsonNode node, String field, int maxLength) {
        if (!node.has(field) || node.get(field).isNull()) return null;
        if (!node.get(field).isTextual()) throw EafException.invalid(field + " 必须是文本。");
        return requireText(node.get(field).asText(), maxLength, field);
    }

    private Instant parseInstant(JsonNode node, String field) {
        try { return Instant.parse(requiredText(node, field, 80)); }
        catch (RuntimeException invalid) { throw EafException.invalid(field + " 必须是 ISO-8601 时间。"); }
    }

    private List<String> stringList(JsonNode node) {
        if (!node.isArray()) throw EafException.invalid("evidenceRefs 必须是数组。");
        var values = new ArrayList<String>();
        node.forEach(value -> {
            if (!value.isTextual()) throw EafException.invalid("evidenceRefs 只能包含文本。");
            values.add(value.asText());
        });
        return List.copyOf(values);
    }

    private MemoryRelease findCandidateRelease(UUID tenantId, UUID workspaceId, UUID candidateId, int revision) {
        return jdbc.query("select release_id, memory_id, memory_version, action, release_origin, candidate_id, candidate_revision, base_version, content_hash, actor_id, occurred_at from memory.release where tenant_id = ? and workspace_id = ? and candidate_id = ? and candidate_revision = ? and action = 'PUBLISHED'",
                rs -> rs.next() ? new MemoryRelease(rs.getObject("release_id", UUID.class), rs.getObject("memory_id", UUID.class),
                        rs.getString("memory_version"), rs.getString("action"), rs.getString("release_origin"),
                        rs.getObject("candidate_id", UUID.class), rs.getObject("candidate_revision", Integer.class),
                        rs.getString("base_version"), rs.getString("content_hash"), rs.getObject("actor_id", UUID.class),
                        rs.getTimestamp("occurred_at").toInstant()) : null,
                tenantId, workspaceId, candidateId, revision);
    }

    private MemoryRelease findCandidateWithdrawal(UUID tenantId, UUID workspaceId, UUID candidateId, int revision) {
        return jdbc.query("select release_id, memory_id, memory_version, action, release_origin, candidate_id, "
                        + "candidate_revision, base_version, content_hash, actor_id, occurred_at from memory.release "
                        + "where tenant_id = ? and workspace_id = ? and action = 'REVOKED' "
                        + "and release_origin = 'LEARNING_CANDIDATE' and candidate_id = ? and candidate_revision = ?",
                rs -> rs.next() ? mapRelease(rs) : null, tenantId, workspaceId, candidateId, revision);
    }

    private MemoryRelease mapRelease(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new MemoryRelease(rs.getObject("release_id", UUID.class), rs.getObject("memory_id", UUID.class),
                rs.getString("memory_version"), rs.getString("action"), rs.getString("release_origin"),
                rs.getObject("candidate_id", UUID.class), rs.getObject("candidate_revision", Integer.class),
                rs.getString("base_version"), rs.getString("content_hash"), rs.getObject("actor_id", UUID.class),
                rs.getTimestamp("occurred_at").toInstant());
    }

    private List<String> readEvidence(String source) {
        try {
            JsonNode refs = json.readTree(source);
            if (refs == null || !refs.isArray()) throw new IllegalStateException("Memory evidence_refs 不是数组。");
            var result = new ArrayList<String>();
            refs.forEach(ref -> result.add(ref.asText()));
            return List.copyOf(result);
        } catch (JsonProcessingException e) { throw new IllegalStateException("Memory 来源无法解析。", e); }
    }

    private String requireText(String value, int maxLength, String field) {
        if (value == null || value.isBlank() || value.length() > maxLength)
            throw EafException.invalid(field + " 缺失或超过长度限制。");
        return value.trim();
    }

    private record PreparedVersion(String type, String scope, String content, double confidence, Instant expiresAt,
                                   String sourceRef, List<String> evidenceRefs, String businessEntityType,
                                   String businessEntityId) { }
    private record MemoryKey(UUID id, String version) { }
    private record MemoryReplayTarget(String status, int attempts, UUID memoryId, String version) { }
    private record MemoryReplayCommand(UUID commandId, UUID eventId, UUID actorId, String requestHash,
                                       String status, int attempts, Instant createdAt) { }
    private record MemoryOutboxPayload(UUID eventId, UUID tenantId, UUID workspaceId, UUID memoryId,
                                       String memoryVersion, String action, String releaseOrigin,
                                       UUID candidateId, Integer candidateRevision, String baseVersion,
                                       String contentHash, UUID actorId) { }
    private record OwnerAndKey(UUID ownerId, String logicalKey) { }
    private record CandidateReleaseRequest(UUID candidateId, int candidateRevision, UUID memoryId,
                                           String baseVersion, String logicalKey, JsonNode proposedContent) { }
    private record MemoryOrigin(UUID releaseId, String baseVersion) { }
    private record MemoryContent(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String logicalKey,
                                 String version, String type, String scope, String businessEntityType,
                                 String businessEntityId, String content, double confidence,
                                 Instant expiresAt, String sourceType, String sourceRef, List<String> evidenceRefs) { }
    private record ExperienceCardData(UUID id, UUID ownerId, String applicability, String customerId,
                                      int latestRevision, Integer activeRevision, long version, Instant activeSince,
                                      Instant createdAt, Instant updatedAt) { }
    private record ExperienceSelection(UUID cardId, String applicability, int revision, String memoryVersion,
                                      String title) { }
    private record ExperienceCommand(String action, UUID cardId, int revision, String memoryVersion,
                                     long cardVersion, String requestHash) { }
    private record ExperienceCreateHash(String title, String content, String type, String applicability,
                                        String customerId, Instant expiresAt, UUID sourceTaskId,
                                        UUID sourceFeedbackId, UUID draftTaskId) { }
    private record ExperienceSaveHash(UUID cardId, long expectedVersion, String title, String content,
                                      String type, Instant expiresAt, UUID sourceTaskId, UUID sourceFeedbackId,
                                      UUID draftTaskId) { }
}
// 本文件负责实现 EAF 的 JdbcMemoryService.java 相关代码。
