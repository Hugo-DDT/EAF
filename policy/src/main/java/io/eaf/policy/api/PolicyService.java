package io.eaf.policy.api;

public interface PolicyService {
    PolicyDecision evaluate(PolicyRequest request);
    PolicyDecision evaluateServiceRequest(ServiceRequestPolicyRequest request);
}
// 本文件负责实现 EAF 的 PolicyService.java 相关代码。
