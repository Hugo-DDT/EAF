package io.eaf.shared;

public final class EafException extends RuntimeException {
    private final int status;
    private final String code;
    private final boolean retryable;

    public EafException(int status, String code, String detail) { this(status, code, detail, false); }

    public EafException(int status, String code, String detail, boolean retryable) {
        super(detail);
        this.status = status;
        this.code = code;
        this.retryable = retryable;
    }

    public int status() { return status; }
    public String code() { return code; }
    public boolean retryable() { return retryable; }

    public static EafException invalid(String detail) { return new EafException(400, "INVALID_REQUEST", detail); }
    public static EafException unauthenticated() { return new EafException(401, "UNAUTHENTICATED", "需要有效的 Bearer 身份。"); }
    public static EafException forbidden(String detail) { return new EafException(403, "POLICY_DENIED", detail); }
    public static EafException notFound() { return new EafException(404, "RESOURCE_NOT_FOUND", "资源不存在或当前身份不可见。"); }
    public static EafException conflict(String code, String detail) { return new EafException(409, code, detail); }
}
// 本文件负责实现 EAF 的 EafException.java 相关代码。
