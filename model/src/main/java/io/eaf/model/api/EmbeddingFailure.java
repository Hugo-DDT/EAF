package io.eaf.model.api;

public final class EmbeddingFailure extends RuntimeException {
    private final String code;
    private final boolean retryable;
    private final boolean called;

    public EmbeddingFailure(String code, String detail, boolean retryable, boolean called) {
        super(detail);
        this.code = code;
        this.retryable = retryable;
        this.called = called;
    }

    public String code() { return code; }
    public boolean retryable() { return retryable; }
    public boolean called() { return called; }
}
// 本异常把预算、授权、协议和上游失败区分开，调用方不能把失败伪装成空向量。
