package io.eaf.secret.api;

/** 秘密只在明确的出站边界短暂取值；调试字符串永远不包含明文。 */
public final class SecretValue {
    private final String value;

    public SecretValue(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("秘密值不能为空。");
        this.value = value;
    }

    public String revealForOutboundRequest() { return value; }

    @Override public String toString() { return "[已隐藏]"; }
}
