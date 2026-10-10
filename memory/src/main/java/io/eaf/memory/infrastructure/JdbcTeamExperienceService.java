package io.eaf.memory.infrastructure;

import io.eaf.memory.api.TeamExperienceService;
import io.eaf.memory.api.TeamExperienceService.CardReceipt;
import io.eaf.memory.api.TeamExperienceService.CreateTeamExperience;
import io.eaf.memory.api.TeamExperienceService.PublishTeamExperience;
import io.eaf.memory.api.TeamExperienceService.RevokeTeamExperience;
import io.eaf.memory.api.TeamExperienceService.SaveTeamExperience;
import io.eaf.memory.api.TeamExperienceService.TeamExperience;
import io.eaf.memory.api.TeamExperienceService.TeamExperiencePage;
import io.eaf.memory.api.TeamExperienceService.TeamExperienceRevision;
import io.eaf.memory.api.TeamExperienceService.TeamExperienceSelection;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** REST 与 Context 使用的 Memory Owner 入口。 */
@Service
public class JdbcTeamExperienceService implements TeamExperienceService {
    private final JdbcMemoryService memory;

    public JdbcTeamExperienceService(JdbcMemoryService memory) { this.memory = memory; }

    @Override public TeamExperiencePage list(ActorContext actor, UUID workspaceId, String scenarioKey, boolean owned, String status,
            Instant cursorUpdatedAt, UUID cursorId, int limit) {
        return memory.listTeam(actor, workspaceId, scenarioKey, owned, status, cursorUpdatedAt, cursorId, limit);
    }
    @Override public TeamExperienceDiscovery discover(ActorContext actor, UUID workspaceId, String scenarioKey,
            List<String> keywords, int limit) {
        return memory.discoverTeam(actor, workspaceId, scenarioKey, keywords, limit);
    }
    @Override public TeamExperience get(ActorContext actor, UUID workspaceId, UUID cardId) {
        return memory.getTeam(actor, workspaceId, cardId);
    }
    @Override public List<TeamExperienceRevision> versions(ActorContext actor, UUID workspaceId, UUID cardId,
            Integer beforeRevision, int limit) {
        return memory.versionsTeam(actor, workspaceId, cardId, beforeRevision, limit);
    }
    @Override public CardReceipt create(CreateTeamExperience command) { return memory.create(command); }
    @Override public CardReceipt save(SaveTeamExperience command) { return memory.save(command); }
    @Override public CardReceipt publish(PublishTeamExperience command) { return memory.publish(command); }
    @Override public CardReceipt revoke(RevokeTeamExperience command) { return memory.revoke(command); }
    @Override public CardReceipt publishCandidate(PublishCandidateTeamExperience command) {
        return memory.publishTeamCandidate(command);
    }
    @Override public Optional<CardReceipt> findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision) {
        return memory.findTeamCandidateRelease(actor, workspaceId, candidateId, candidateRevision);
    }
    @Override public CardReceipt revokeCandidate(RevokeCandidateTeamExperience command) {
        return memory.revokeTeamCandidate(command);
    }
    @Override public Optional<CardReceipt> findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision) {
        return memory.findTeamCandidateWithdrawal(actor, workspaceId, candidateId, candidateRevision);
    }
    @Override public TeamExperienceSelection requireCurrent(ActorContext actor, UUID workspaceId, String scenarioKey,
            UUID cardId, int revision) {
        return memory.requireCurrent(actor, workspaceId, scenarioKey, cardId, revision);
    }
    @Override public Optional<CardReceipt> replayCreate(CreateTeamExperience command) { return memory.replayCreate(command); }
    @Override public Optional<CardReceipt> replaySave(SaveTeamExperience command) { return memory.replaySave(command); }
    @Override public Optional<CardReceipt> replayPublish(PublishTeamExperience command) { return memory.replayPublish(command); }
}
