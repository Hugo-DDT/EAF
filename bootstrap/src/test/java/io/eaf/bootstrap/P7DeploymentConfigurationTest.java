package io.eaf.bootstrap;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 核验企业启动配置及其防降级约束，不连接真实 IdP、秘密服务或数据库。 */
class P7DeploymentConfigurationTest {

    @Test
    void enterpriseProfileRequiresIdentityModelAndIndependentDatabaseCredentials() throws IOException {
        // 读取应用实际使用的 profile YAML，确认安全缺省与必填配置没有漂移。
        var source = new YamlPropertySourceLoader()
                .load("enterprise", new ClassPathResource("application-enterprise.yml"))
                .getFirst();

        assertThat(source.getProperty("spring.config.activate.on-profile")).isEqualTo("enterprise");
        assertThat(source.getProperty("spring.flyway.user").toString()).contains("EAF_DB_MIGRATOR_USERNAME");
        assertThat(source.getProperty("spring.flyway.password").toString()).contains("EAF_DB_MIGRATOR_PASSWORD");
        assertThat(source.getProperty("eaf.security.mode")).isEqualTo("enterprise");
        assertThat(source.getProperty("eaf.security.enterprise.issuer").toString()).contains("EAF_IDP_ISSUER");
        assertThat(source.getProperty("eaf.security.enterprise.jwks-uri").toString()).contains("EAF_IDP_JWKS_URI");
        assertThat(source.getProperty("eaf.security.enterprise.audience").toString()).contains("EAF_IDP_AUDIENCE");
        assertThat(source.getProperty("eaf.secrets.backend").toString()).contains("unconfigured");
        assertThat(source.getProperty("eaf.model.mode").toString()).contains("EAF_MODEL_MODE");
        assertThat(source.getProperty("eaf.outbound.enterprise.egress-policy-confirmed").toString()).contains(":false");
        assertThat(source.getProperty("eaf.agent-protocol.mcp-enabled").toString()).contains(":false");
        assertThat(source.getProperty("eaf.agent-protocol.a2a-enabled").toString()).contains(":false");
    }

    @Test
    void enterpriseStartupAcceptsOnlyEnterpriseIdentityAndDistinctDatabaseRoles() {
        // 通过不降级的企业参数时守卫通过；迁移角色与运行角色由部署端分别授予最小权限。
        var environment = new MockEnvironment();
        environment.setActiveProfiles("enterprise");
        var invariant = new EnterpriseDeploymentInvariant(environment, "enterprise", "unconfigured",
                "eaf_runtime", "eaf_migrator");

        invariant.afterSingletonsInstantiated();
    }

    @Test
    void enterpriseStartupRejectsSecurityDowngradeEnvironmentSecretsAndSharedDatabaseRole() {
        // 高优先级配置不得把企业入口降为 local、引入 Environment Secret，或复用 DDL 运行账户。
        var environment = new MockEnvironment();
        environment.setActiveProfiles("enterprise");
        assertThatThrownBy(() -> new EnterpriseDeploymentInvariant(environment, "local", "unconfigured",
                "eaf_runtime", "eaf_migrator").afterSingletonsInstantiated())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("enterprise 身份模式");
        assertThatThrownBy(() -> new EnterpriseDeploymentInvariant(environment, "enterprise", "environment",
                "eaf_runtime", "eaf_migrator").afterSingletonsInstantiated())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Environment");
        assertThatThrownBy(() -> new EnterpriseDeploymentInvariant(environment, "enterprise", "unconfigured",
                "eaf_runtime", "EAF_RUNTIME").afterSingletonsInstantiated())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("独立");
    }
}
