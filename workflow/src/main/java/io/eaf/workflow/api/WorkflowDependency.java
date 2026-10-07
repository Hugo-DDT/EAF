package io.eaf.workflow.api;

import java.util.UUID;

// 已发布版本保存精确 Capability 内容摘要，草稿摘要在发布校验后补齐。
public record WorkflowDependency(UUID capabilityId, String capabilityVersion, String contentHash) { }
