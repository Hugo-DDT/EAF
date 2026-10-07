package io.eaf.model.api;

// 受信 Provider 在出站前声明计价身份和调用类别；响应不能自行选择价格目录。
public record ModelBillingProfile(String provider, String model, String callType) {
    public ModelBillingProfile(String provider, String model) {
        this(provider, model, "CHAT");
    }
}
