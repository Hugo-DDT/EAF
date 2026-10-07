package io.eaf.memory.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 个人经验卡的 Memory 域所有权入口；经验仍以 Memory 版本与发布事实存储。 */
public interface ExperienceCardService {
    CardPage list(ActorContext actor, UUID workspaceId, String applicability, String customerId,
                  String status, Instant cursorUpdatedAt, UUID cursorId, int limit);
    ExperienceCard get(ActorContext actor, UUID workspaceId, UUID cardId);
    List<ExperienceCardRevision> versions(ActorContext actor, UUID workspaceId, UUID cardId,
                                          Integer beforeRevision, int limit);
    CardReceipt create(CreateExperienceCard command);
    CardReceipt save(SaveExperienceCardVersion command);
    CardReceipt publish(PublishExperienceCard command);
    CardReceipt revoke(RevokeExperienceCard command);
    List<ApplicableExperience> findApplicable(ActorContext actor, UUID workspaceId, String customerId, int limit);

    record CreateExperienceCard(ActorContext actor, UUID workspaceId, String title, String content, String type,
                                String applicability, String customerId, Instant expiresAt, UUID sourceTaskId,
                                UUID sourceFeedbackId, UUID draftTaskId, String idempotencyKey) { }
    record SaveExperienceCardVersion(ActorContext actor, UUID workspaceId, UUID cardId, long expectedVersion,
                                     String title, String content, String type, Instant expiresAt,
                                     UUID sourceTaskId, UUID sourceFeedbackId, UUID draftTaskId,
                                     String idempotencyKey) { }
    record PublishExperienceCard(ActorContext actor, UUID workspaceId, UUID cardId, int revision,
                                 long expectedVersion, String idempotencyKey) { }
    record RevokeExperienceCard(ActorContext actor, UUID workspaceId, UUID cardId, long expectedVersion,
                                int expectedActiveRevision, String idempotencyKey) { }
    record CardReceipt(UUID cardId, String action, int revision, String memoryVersion,
                       long cardVersion, boolean replayed) { }
    record CardPage(List<ExperienceCard> items, Instant nextCursorUpdatedAt, UUID nextCursorId) {
        public CardPage { items = items == null ? List.of() : List.copyOf(items); }
    }
    record ExperienceCard(UUID id, UUID ownerId, String applicability, String customerId,
                          int latestRevision, Integer activeRevision, long version, String status,
                          Instant activeSince, Instant createdAt, Instant updatedAt,
                          ExperienceCardRevision latest, ExperienceCardRevision active) { }
    record ExperienceCardRevision(int revision, String memoryVersion, String title, String content,
                                  String type, String status, Instant expiresAt, UUID sourceTaskId,
                                  UUID sourceFeedbackId, UUID draftTaskId, Instant createdAt) { }
    record ApplicableExperience(UUID cardId, int revision, String memoryVersion, String title,
                                 String applicability, MemoryDefinition memory) { }
}
