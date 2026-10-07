package io.eaf.agentprotocol;

import io.eaf.learning.api.FeedbackService;
import io.eaf.memory.api.ExperienceCardService;
import io.eaf.memory.api.ExperienceCardService.CardReceipt;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** 协议层核验反馈/Task 归属，再把可信来源引用交给 Memory。 */
@Service
public class ExperienceCardApplicationService {
    private final ExperienceCardService cards;
    private final FeedbackService feedback;
    private final TaskService tasks;

    public ExperienceCardApplicationService(ExperienceCardService cards, FeedbackService feedback, TaskService tasks) {
        this.cards = cards;
        this.feedback = feedback;
        this.tasks = tasks;
    }

    public CardReceipt create(ActorContext actor, UUID workspaceId, CreateRequest request, String idempotencyKey) {
        var source = verifySource(actor, workspaceId, request.sourceTaskId(), request.sourceFeedbackId());
        verifyDraft(actor, workspaceId, request.draftTaskId(), source);
        return cards.create(new ExperienceCardService.CreateExperienceCard(actor, workspaceId, request.title(),
                request.content(), request.type(), request.applicability(), request.customerId(), request.expiresAt(),
                source == null ? null : source.taskId(), source == null ? null : source.id(), request.draftTaskId(), idempotencyKey));
    }

    public CardReceipt save(ActorContext actor, UUID workspaceId, UUID cardId, SaveRequest request,
                            String idempotencyKey) {
        var source = verifySource(actor, workspaceId, request.sourceTaskId(), request.sourceFeedbackId());
        verifyDraft(actor, workspaceId, request.draftTaskId(), source);
        return cards.save(new ExperienceCardService.SaveExperienceCardVersion(actor, workspaceId, cardId,
                request.expectedVersion(), request.title(), request.content(), request.type(), request.expiresAt(),
                source == null ? null : source.taskId(), source == null ? null : source.id(), request.draftTaskId(), idempotencyKey));
    }

    private io.eaf.learning.api.Feedback verifySource(ActorContext actor, UUID workspaceId,
                                                      UUID sourceTaskId, UUID sourceFeedbackId) {
        if (sourceTaskId == null && sourceFeedbackId == null) return null;
        if (sourceTaskId == null || sourceFeedbackId == null)
            throw EafException.invalid("反馈来源必须同时提供 sourceTaskId 与 sourceFeedbackId。");
        var source = feedback.requireExperienceSource(actor, workspaceId, sourceFeedbackId);
        if (!source.taskId().equals(sourceTaskId)) throw EafException.notFound();
        return source;
    }

    private void verifyDraft(ActorContext actor, UUID workspaceId, UUID draftTaskId,
                             io.eaf.learning.api.Feedback source) {
        if (draftTaskId == null) return;
        if (source == null) throw EafException.invalid("draftTaskId 必须与反馈和来源 Task 一起提供。");
        var binding = tasks.experienceDraftBinding(actor, workspaceId, draftTaskId);
        var draft = tasks.get(actor, workspaceId, draftTaskId);
        if (!actor.actorId().equals(binding.ownerId()) || !source.taskId().equals(binding.sourceTaskId())
                || !source.id().equals(binding.sourceFeedbackId()) || draft.status() != TaskStatus.SUCCEEDED)
            throw EafException.conflict("EXPERIENCE_DRAFT_SOURCE_MISMATCH", "整理 Task 必须成功且绑定当前反馈和来源 Task。");
    }

    public record CreateRequest(String title, String content, String type, String applicability, String customerId,
                                java.time.Instant expiresAt, UUID sourceTaskId, UUID sourceFeedbackId,
                                UUID draftTaskId) { }
    public record SaveRequest(long expectedVersion, String title, String content, String type,
                              java.time.Instant expiresAt, UUID sourceTaskId, UUID sourceFeedbackId,
                              UUID draftTaskId) { }
}
