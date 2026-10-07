package io.eaf.model.infrastructure;

import java.io.IOException;

/** 数据库未能确定共享 Provider 槽状态；拦截器必须在发送前失败关闭。 */
public final class ProviderQuotaUnavailableException extends IOException {
    public ProviderQuotaUnavailableException() { super("Provider 共享并发协调不可用，未发送请求。"); }
}
