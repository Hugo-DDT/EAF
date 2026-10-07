package io.eaf.model.api;

public class ModelFailure extends RuntimeException {
    private final String code;
    private final boolean timeout;
    private final boolean called;

    public ModelFailure(String code, String detail, boolean timeout, boolean called) {
        super(detail);
        this.code = code;
        this.timeout = timeout;
        this.called = called;
    }

    public String code() { return code; }
    public boolean timeout() { return timeout; }
    public boolean called() { return called; }
}
// 本文件负责实现 EAF 的 ModelFailure.java 相关代码。
