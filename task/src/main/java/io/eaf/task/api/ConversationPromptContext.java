package io.eaf.task.api;

import java.util.List;
import java.util.UUID;
import java.time.Instant;

/** Runtime 只读取在轮次事务内固定的对话资料，不回查或重建会话历史。 */
public record ConversationPromptContext(UUID conversationId, UUID turnId, String mode, String customerId,
                                        int briefRevision, String confirmedBrief,
                                        List<HistoryTurn> history, int omittedTurnCount,
                                        String retrievalQuery, String clarificationQuestion,
                                        List<SelectedFollowupResult> followupContext) {
    public ConversationPromptContext {
        history = history == null ? List.of() : List.copyOf(history);
        followupContext = followupContext == null ? List.of() : List.copyOf(followupContext);
    }

    /** 保留既有会话上下文构造方式，未带团队结果的快照仍可读取。 */
    public ConversationPromptContext(UUID conversationId, UUID turnId, String mode, String customerId,
                                     int briefRevision, String confirmedBrief, List<HistoryTurn> history,
                                     int omittedTurnCount, String retrievalQuery, String clarificationQuestion) {
        this(conversationId, turnId, mode, customerId, briefRevision, confirmedBrief, history,
                omittedTurnCount, retrievalQuery, clarificationQuestion, List.of());
    }

    public record HistoryTurn(UUID turnId, UUID taskId, String input, String publicSummary) { }
    public record SelectedFollowupResult(UUID resultId, UUID followupId, int resultNo, UUID recordedBy,
                                         UUID cardCreatorId, String outcomeCode, String summary,
                                         String nextAction, Instant nextContactAt, String disposition,
                                         UUID correctsResultId, Instant createdAt, String syncStatus) { }
}
