package io.eaf.organization.api;

import java.util.UUID;

public record Membership(UUID subjectId, UUID tenantId, boolean active) { }
// 本文件负责实现 EAF 的 Membership.java 相关代码。
