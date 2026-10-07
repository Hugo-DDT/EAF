package io.eaf.credential.api;

/** 只在实际出站代码中暴露凭据；日志、断言失败和调试字符串仅显示版本元数据。 */
public final class ResolvedCredential {
    private final String credentialRef;
    private final long version;
    private final String value;

    public ResolvedCredential(String credentialRef, long version, String value) {
        this.credentialRef = credentialRef;
        this.version = version;
        this.value = value;
    }

    public String credentialRef() { return credentialRef; }
    public long version() { return version; }
    public String valueForOutboundRequest() { return value; }

    @Override public String toString() { return "ResolvedCredential[credentialRef=" + credentialRef + ", version=" + version + ", value=[已隐藏]]"; }
}
