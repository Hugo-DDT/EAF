package io.eaf.memory.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

// 公开 API 保留 Memory 领域所有权；Context 与 Learning 不能直接写本域表。
public interface MemoryService {
    MemoryDefinition create(CreateMemoryCommand command);
    MemoryDefinition addVersion(ActorContext actor, UUID workspaceId, UUID memoryId, CreateMemoryVersionCommand command);
    List<MemoryDefinition> list(ActorContext actor, UUID workspaceId);
    // 只返回本域发布 outbox 的状态引用；无当前记忆读取权限时由 Owner 拒绝。
    MemoryOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                          Instant createdAfter, Instant cursorCreatedAt,
                                          UUID cursorEventId, int pageSize);
    /** 只将本域 FAILED 发布事实重置为 PENDING，保留原 eventId、payload 和尝试次数。 */
    MemoryOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                           String requestKey, String reason);
    // Context 只通过此只读入口取得当前已发布且适用的版本，不访问 Memory 表。
    List<MemoryDefinition> findApplicable(ActorContext actor, UUID workspaceId, String businessEntityType,
                                         String businessEntityId);
    MemoryDefinition get(ActorContext actor, UUID workspaceId, UUID memoryId, String version);
    MemoryDefinition requireUsable(ActorContext actor, UUID workspaceId, UUID memoryId, String version);
    MemoryDefinition publish(ActorContext actor, UUID workspaceId, UUID memoryId, String version, long expectedVersion);
    // 学习发布以候选修订作唯一幂等键，版本与发布事实在 Memory 本域事务内原子提交。
    MemoryRelease publishCandidate(MemoryCandidateReleaseCommand command);
    MemoryDefinition revoke(ActorContext actor, UUID workspaceId, UUID memoryId, String version, long expectedVersion);
    // Learning 撤回只允许撤回来源完全匹配的具体候选版本，并返回可恢复的撤回事实。
    MemoryRelease revokeCandidate(ActorContext actor, UUID workspaceId, UUID memoryId, String version,
                                  long expectedVersion, UUID candidateId, int candidateRevision);
    // 来源键只恢复本域发布结果，Memory 仍自行执行 Owner/Scope 可见性校验。
    MemoryRelease findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId, int candidateRevision);
    MemoryRelease findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId, int candidateRevision);
}
