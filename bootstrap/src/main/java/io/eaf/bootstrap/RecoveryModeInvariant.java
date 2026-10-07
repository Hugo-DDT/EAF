package io.eaf.bootstrap;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 校验恢复 profile 的最终解析配置，避免环境变量重新开启调度或外部网络。 */
@Component
public final class RecoveryModeInvariant implements SmartInitializingSingleton {
    private final boolean recoveryMode;
    private final String securityMode;
    private final String secretBackend;
    private final boolean egressPolicyConfirmed;
    private final boolean taskDispatcherEnabled;
    private final boolean workflowDispatcherEnabled;
    private final boolean executionOutboxEnabled;
    private final boolean executionRemotePollerEnabled;
    private final boolean knowledgeOutboxEnabled;
    private final boolean memoryOutboxEnabled;

    public RecoveryModeInvariant(@Value("${eaf.recovery.mode:false}") boolean recoveryMode,
                                 @Value("${eaf.security.mode:disabled}") String securityMode,
                                 @Value("${eaf.secrets.backend:unconfigured}") String secretBackend,
                                 @Value("${eaf.outbound.enterprise.egress-policy-confirmed:false}") boolean egressPolicyConfirmed,
                                 @Value("${eaf.task.dispatcher-enabled:true}") boolean taskDispatcherEnabled,
                                 @Value("${eaf.workflow.dispatcher-enabled:true}") boolean workflowDispatcherEnabled,
                                 @Value("${eaf.execution.outbox-publisher-enabled:true}") boolean executionOutboxEnabled,
                                 @Value("${eaf.execution.remote-poller-enabled:true}") boolean executionRemotePollerEnabled,
                                 @Value("${eaf.knowledge.outbox-publisher-enabled:true}") boolean knowledgeOutboxEnabled,
                                 @Value("${eaf.memory.outbox-publisher-enabled:true}") boolean memoryOutboxEnabled) {
        this.recoveryMode = recoveryMode;
        this.securityMode = securityMode;
        this.secretBackend = secretBackend;
        this.egressPolicyConfirmed = egressPolicyConfirmed;
        this.taskDispatcherEnabled = taskDispatcherEnabled;
        this.workflowDispatcherEnabled = workflowDispatcherEnabled;
        this.executionOutboxEnabled = executionOutboxEnabled;
        this.executionRemotePollerEnabled = executionRemotePollerEnabled;
        this.knowledgeOutboxEnabled = knowledgeOutboxEnabled;
        this.memoryOutboxEnabled = memoryOutboxEnabled;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!recoveryMode) return;
        // 在任何 ApplicationReadyEvent 调度恢复前验证最终配置；不允许高优先级环境变量悄悄解除屏障。
        if (!"enterprise".equalsIgnoreCase(securityMode) || "environment".equalsIgnoreCase(secretBackend)
                || egressPolicyConfirmed
                || taskDispatcherEnabled || workflowDispatcherEnabled || executionOutboxEnabled
                || executionRemotePollerEnabled || knowledgeOutboxEnabled || memoryOutboxEnabled)
            throw new IllegalStateException("恢复配置必须使用企业身份与不可回退的秘密后端、关闭出站，并禁用任务/流程调度及事件投递器。");
    }
}
