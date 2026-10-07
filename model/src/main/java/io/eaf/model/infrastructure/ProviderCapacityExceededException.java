package io.eaf.model.infrastructure;

import java.io.IOException;

/** Provider 请求尚未发送时，本地共享出站额度已满。 */
public final class ProviderCapacityExceededException extends IOException {
    public ProviderCapacityExceededException() { super("Provider 出站并发已满。"); }
}
