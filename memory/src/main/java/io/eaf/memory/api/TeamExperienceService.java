package io.eaf.memory.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** 同 Workspace 服务请求经验；发布仍通过 Memory 版本与事实存储。 */
public interface TeamExperienceService {
    TeamExperiencePage list(ActorContext actor, UUID workspaceId, String scenarioKey, boolean owned, String status,
                            Instant cursorUpdatedAt, UUID cursorId, int limit);
    TeamExperienceDiscovery discover(ActorContext actor, UUID workspaceId, String scenarioKey, List<String> keywords, int limit);
    TeamExperience get(ActorContext actor, UUID workspaceId, UUID cardId);
    List<TeamExperienceRevision> versions(ActorContext actor, UUID workspaceId, UUID cardId,
                                          Integer beforeRevision, int limit);
    CardReceipt create(CreateTeamExperience command);
    CardReceipt save(SaveTeamExperience command);
    CardReceipt publish(PublishTeamExperience command);
    CardReceipt revoke(RevokeTeamExperience command);
    /** Learning 候选由 Memory 在本域事务内直接形成正式 TEAM 修订，不经过可手工发布的 DRAFT。 */
    CardReceipt publishCandidate(PublishCandidateTeamExperience command);
    Optional<CardReceipt> findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                               int candidateRevision);
    CardReceipt revokeCandidate(RevokeCandidateTeamExperience command);
    Optional<CardReceipt> findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                  int candidateRevision);
    TeamExperienceSelection requireCurrent(ActorContext actor, UUID workspaceId, String scenarioKey,
                                           UUID cardId, int revision);
    Optional<CardReceipt> replayCreate(CreateTeamExperience command);
    Optional<CardReceipt> replaySave(SaveTeamExperience command);
    Optional<CardReceipt> replayPublish(PublishTeamExperience command);

    record SourceProof(UUID workItemId, UUID instanceId, long workItemVersion, UUID completedBy,
                       Instant completedAt, String outcome, String resultHash, String sourceType) { }
    record ExperienceRef(UUID cardId, int revision) { }
    record TeamExperienceSelection(UUID cardId, int revision, String memoryVersion, String title,
                                   String appliesWhen, String content, MemoryDefinition memory) { }
    record CreateTeamExperience(ActorContext actor, UUID workspaceId, String scenarioKey, String title,
            String appliesWhen, String content, Instant expiresAt, UUID sourceWorkItemId, SourceProof source,
            String idempotencyKey) { }
    record SaveTeamExperience(ActorContext actor, UUID workspaceId, UUID cardId, long expectedVersion,
            String title, String appliesWhen, String content, Instant expiresAt, UUID sourceWorkItemId, SourceProof source,
            String idempotencyKey) { }
    record PublishTeamExperience(ActorContext actor, UUID workspaceId, UUID cardId, int revision,
            long expectedVersion, SourceProof source, String idempotencyKey) { }
    record RevokeTeamExperience(ActorContext actor, UUID workspaceId, UUID cardId, long expectedVersion,
                                int expectedActiveRevision, String idempotencyKey) { }
    record PublishCandidateTeamExperience(ActorContext actor, UUID workspaceId, UUID cardId, UUID candidateId,
            int candidateRevision, int baseRevision, String baseMemoryVersion, long expectedCardVersion,
            String title, String appliesWhen, String content, String contentHash, String idempotencyKey) { }
    record RevokeCandidateTeamExperience(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, String idempotencyKey) { }
    record TeamExperiencePage(List<TeamExperience> items, Instant nextCursorUpdatedAt, UUID nextCursorId) {
        public TeamExperiencePage { items = items == null ? List.of() : List.copyOf(items); }
    }
    record TeamExperienceDiscovery(String algorithmVersion, List<DiscoveredTeamExperience> items) {
        public TeamExperienceDiscovery { items = items == null ? List.of() : List.copyOf(items); }
    }
    record DiscoveredTeamExperience(UUID cardId, int revision, String memoryVersion, String contentHash,
            String title, String appliesWhen, int score, List<String> matchedTerms, List<String> matchedFields,
            String preview, Instant expiresAt, TeamExperienceSourceSummary source) {
        public DiscoveredTeamExperience {
            matchedTerms = List.copyOf(matchedTerms);
            matchedFields = List.copyOf(matchedFields);
        }
    }
    record TeamExperienceSourceSummary(UUID workItemId, String outcome, Instant completedAt) { }
    record TeamExperience(UUID id, UUID ownerId, String scenarioKey, int latestRevision,
            Integer activeRevision, long version, String status, Instant updatedAt,
            TeamExperienceRevision latest, TeamExperienceRevision active) { }
    record TeamExperienceRevision(int revision, String memoryVersion, String title, String appliesWhen,
            String content, String status, Instant expiresAt, SourceProof source, String contentHash,
            Instant createdAt, UUID originCandidateId, Integer originCandidateRevision) { }
    record CardReceipt(UUID cardId, String action, int revision, String memoryVersion,
                       long cardVersion, boolean replayed) { }
}
