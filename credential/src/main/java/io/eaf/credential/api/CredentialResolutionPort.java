package io.eaf.credential.api;

/** Integration 与 Model 的受限出站解析端口；解析过程每次读取当前元数据版本。 */
public interface CredentialResolutionPort {
    ResolvedCredential resolve(CredentialRequest request);

    /** Model 只能读取预登记的全局 Provider 用途凭据；不能访问 Workspace Connector 凭据。 */
    ResolvedCredential resolveSystem(String credentialRef, String audience, String use, String permission);
}
