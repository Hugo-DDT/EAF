package io.eaf.agentprotocol;

import io.eaf.memory.api.TeamExperienceService;
import io.eaf.memory.api.TeamExperienceService.CardReceipt;
import io.eaf.memory.api.TeamExperienceService.CreateTeamExperience;
import io.eaf.memory.api.TeamExperienceService.PublishTeamExperience;
import io.eaf.memory.api.TeamExperienceService.RevokeTeamExperience;
import io.eaf.memory.api.TeamExperienceService.SaveTeamExperience;
import io.eaf.shared.ActorContext;
import io.eaf.workflow.api.WorkflowService;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** 协议层只接受 sourceWorkItemId；完成事实由 Workflow 核验后再交给 Memory。 */
@Service
public class TeamExperienceApplicationService {
    private final TeamExperienceService experiences;
    private final WorkflowService workflows;

    public TeamExperienceApplicationService(TeamExperienceService experiences, WorkflowService workflows) {
        this.experiences = experiences;
        this.workflows = workflows;
    }

    public CardReceipt create(ActorContext actor, UUID workspaceId, CreateRequest request, String idempotencyKey) {
        var replay = new CreateTeamExperience(actor, workspaceId, request.scenarioKey(), request.title(),
                request.appliesWhen(), request.content(), request.expiresAt(), request.sourceWorkItemId(), null,
                idempotencyKey);
        var prior = experiences.replayCreate(replay);
        if (prior.isPresent()) return prior.get();
        var source = source(actor, workspaceId, request.sourceWorkItemId());
        return experiences.create(new CreateTeamExperience(actor, workspaceId, request.scenarioKey(), request.title(),
                request.appliesWhen(), request.content(), request.expiresAt(), request.sourceWorkItemId(), source, idempotencyKey));
    }

    public CardReceipt save(ActorContext actor, UUID workspaceId, UUID cardId, SaveRequest request,
            String idempotencyKey) {
        var replay = new SaveTeamExperience(actor, workspaceId, cardId, request.expectedVersion(), request.title(),
                request.appliesWhen(), request.content(), request.expiresAt(), request.sourceWorkItemId(), null,
                idempotencyKey);
        var prior = experiences.replaySave(replay);
        if (prior.isPresent()) return prior.get();
        var source = source(actor, workspaceId, request.sourceWorkItemId());
        return experiences.save(new SaveTeamExperience(actor, workspaceId, cardId, request.expectedVersion(),
                request.title(), request.appliesWhen(), request.content(), request.expiresAt(),
                request.sourceWorkItemId(), source, idempotencyKey));
    }

    public CardReceipt publish(ActorContext actor, UUID workspaceId, UUID cardId, int revision, long expectedVersion,
            String idempotencyKey) {
        var replay = new PublishTeamExperience(actor, workspaceId, cardId, revision, expectedVersion, null, idempotencyKey);
        var prior = experiences.replayPublish(replay);
        if (prior.isPresent()) return prior.get();
        var card = experiences.get(actor, workspaceId, cardId);
        if (card.latest() == null || card.latest().revision() != revision)
            throw io.eaf.shared.EafException.conflict("EXPERIENCE_VERSION_CONFLICT", "只能发布当前最新团队经验草稿。");
        var source = source(actor, workspaceId, card.latest().source().workItemId());
        return experiences.publish(new PublishTeamExperience(actor, workspaceId, cardId, revision, expectedVersion,
                source, idempotencyKey));
    }

    public CardReceipt revoke(ActorContext actor, UUID workspaceId, UUID cardId, RevokeRequest request,
            String idempotencyKey) {
        return experiences.revoke(new RevokeTeamExperience(actor, workspaceId, cardId, request.expectedVersion(),
                request.expectedActiveRevision(), idempotencyKey));
    }

    private TeamExperienceService.SourceProof source(ActorContext actor, UUID workspaceId, UUID workItemId) {
        var proof = workflows.requireTeamExperienceSource(actor, workspaceId, workItemId);
        return new TeamExperienceService.SourceProof(proof.workItemId(), proof.instanceId(), proof.workItemVersion(),
                proof.completedBy(), proof.completedAt(), proof.outcome(), proof.resultHash(), proof.sourceType());
    }

    public record CreateRequest(String scenarioKey, String title, String appliesWhen, String content,
                                java.time.Instant expiresAt, UUID sourceWorkItemId) { }
    public record SaveRequest(long expectedVersion, String title, String appliesWhen, String content,
                              java.time.Instant expiresAt, UUID sourceWorkItemId) { }
    public record RevokeRequest(long expectedVersion, int expectedActiveRevision) { }
}
