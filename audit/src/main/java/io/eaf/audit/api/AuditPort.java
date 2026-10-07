package io.eaf.audit.api;

public interface AuditPort {
    void append(AuditFact fact);
}
// 本文件负责实现 EAF 的 AuditPort.java 相关代码。
