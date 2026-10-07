package io.eaf.connector.api;

/** CRM 结果写入的三态回执，UNKNOWN 必须沿原 operationId 回读。 */
public record ExternalOutcomeWriteResult(String state, FollowupOutcomeRecord record,
                                         String errorCode, String detail) {
    public static ExternalOutcomeWriteResult accepted(FollowupOutcomeRecord record) {
        return new ExternalOutcomeWriteResult("ACCEPTED", record, null, null);
    }
    public static ExternalOutcomeWriteResult unknown(String code, String detail) {
        return new ExternalOutcomeWriteResult("UNKNOWN", null, code, detail);
    }
    public static ExternalOutcomeWriteResult rejected(String code, String detail) {
        return new ExternalOutcomeWriteResult("REJECTED", null, code, detail);
    }
}
