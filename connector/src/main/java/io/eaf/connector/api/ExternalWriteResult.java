package io.eaf.connector.api;

/** 外部写请求的三态结果；UNKNOWN 绝不能被调用方当成可安全重试。 */
public record ExternalWriteResult(String state, FollowupRecord record, String errorCode, String detail) {
    public static ExternalWriteResult accepted(FollowupRecord record) { return new ExternalWriteResult("ACCEPTED", record, null, null); }
    public static ExternalWriteResult unknown(String code, String detail) { return new ExternalWriteResult("UNKNOWN", null, code, detail); }
    public static ExternalWriteResult rejected(String code, String detail) { return new ExternalWriteResult("REJECTED", null, code, detail); }
}
