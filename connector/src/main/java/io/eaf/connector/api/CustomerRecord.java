package io.eaf.connector.api;

import java.time.Instant;

public record CustomerRecord(String customerId, String renewalStatus, String lastContactDate,
                             String complaintSummary, String sourceId, String externalVersion, Instant readAt) { }
// 本文件负责实现 EAF 的 CustomerRecord.java 相关代码。
