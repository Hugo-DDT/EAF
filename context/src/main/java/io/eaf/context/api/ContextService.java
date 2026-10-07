package io.eaf.context.api;

import io.eaf.shared.ActorContext;
import io.eaf.memory.api.TeamExperienceService.ExperienceRef;
import java.util.List;
import java.util.UUID;

public interface ContextService {
    EnterpriseContext query(ActorContext actor, UUID workspaceId, ContextQuery request);

    // 只有服务端 Runtime 路径可附加已领取 Task 的计量身份；REST 入口仍使用无 Task 的独立 JOB scope。
    default EnterpriseContext query(ActorContext actor, UUID workspaceId, ContextQuery request, ContextTaskScope taskScope) {
        return query(actor, workspaceId, request);
    }

    // Runtime 可先准备 Knowledge 候选，再在服务端按证据判断结果组装最终上下文。
    default EnterpriseContext prepare(ActorContext actor, UUID workspaceId, ContextQuery request, ContextTaskScope taskScope) {
        return query(actor, workspaceId, request, taskScope);
    }

    default EnterpriseContext assemble(ActorContext actor, UUID workspaceId, EnterpriseContext candidates,
                                       java.util.Map<String, String> selections, ContextQuery request,
                                       ContextTaskScope taskScope) {
        return candidates;
    }

    default EnterpriseContext resolveTeamExperiences(ActorContext actor, UUID workspaceId, String scenarioKey,
                                                      List<ExperienceRef> selections) {
        throw new UnsupportedOperationException("Team experience context is unavailable.");
    }

    // Runtime 在每轮模型提交前调用，防止撤回或权限变化后继续使用旧快照。
    boolean isCurrent(ActorContext actor, UUID workspaceId, EnterpriseContext context);
}
// context 通过 knowledge 的公开检索 API 获取数据，禁止直接依赖 knowledge 的 JDBC 实现。
