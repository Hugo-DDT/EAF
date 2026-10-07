package io.eaf.model.api;

public interface ModelGateway {
    ModelResult call(ModelRequest request);
    int callCount();

    // 仅真实计费 Provider 返回身份；Runtime 据此在出站前向 Usage 申请金额预留。
    default ModelBillingProfile billingProfile() { return null; }

    // 真实 Provider 声明启动配置批准的合成数据范围；Runtime 按本轮实际输入再次匹配。
    default String outboundDataScope() { return null; }
}
// 本文件负责实现 EAF 的 ModelGateway.java 相关代码。
