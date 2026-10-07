package io.eaf.secret.api;

/** 仅受信模块可按后端引用请求秘密；调用方不得持久化或记录返回值。 */
public interface SecretResolver {
    SecretValue resolve(String secretRef);
}
