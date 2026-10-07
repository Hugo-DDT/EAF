package io.eaf.connector.api;

public record ServiceRequestWriteResult(String state, String errorCode, String detail,
                                        ServiceRequestReceipt receipt) {
    public static ServiceRequestWriteResult accepted(ServiceRequestReceipt receipt) {
        return new ServiceRequestWriteResult("ACCEPTED", null, null, receipt);
    }
    public static ServiceRequestWriteResult unknown(String code, String detail) {
        return new ServiceRequestWriteResult("UNKNOWN", code, detail, null);
    }
    public static ServiceRequestWriteResult rejected(String code, String detail) {
        return new ServiceRequestWriteResult("REJECTED", code, detail, null);
    }
}
