package io.eaf.policy.api;

public record PolicyDecision(boolean allowed, String policyVersion, String reason) { }
// 本文件负责实现 EAF 的 PolicyDecision.java 相关代码。
