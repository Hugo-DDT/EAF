package io.eaf.secret.infrastructure;

import io.eaf.secret.api.SecretResolver;
import io.eaf.secret.api.SecretValue;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
public class SecretResolverConfiguration {
    @Bean
    @ConditionalOnProperty(name = "eaf.secrets.backend", havingValue = "environment")
    SecretResolver environmentSecretResolver(Environment environment) {
        return new EnvironmentSecretResolver(environment);
    }

    @Bean
    @ConditionalOnMissingBean(SecretResolver.class)
    SecretResolver unavailableSecretResolver() {
        // 未登记秘密后端时明确失败，不回退到明文配置或空凭据。
        return secretRef -> { throw new IllegalStateException("秘密后端不可用。"); };
    }

    private static final class EnvironmentSecretResolver implements SecretResolver {
        private final Environment properties;

        private EnvironmentSecretResolver(Environment properties) {
            this.properties = properties;
        }

        @Override
        public SecretValue resolve(String secretRef) {
            // Environment 后端仅供非企业开发配置；enterprise 必须换成受管秘密服务。
            if ("enterprise".equalsIgnoreCase(properties.getProperty("eaf.security.mode", "disabled")))
                throw new IllegalStateException("企业模式未配置受管秘密后端。");
            if (secretRef == null || !secretRef.matches("env://(?:eaf\\.credentials\\.[a-z0-9._-]+\\.token|[A-Z][A-Z0-9_]{0,119})"))
                throw new IllegalStateException("秘密引用无效。");
            var key = secretRef.substring("env://".length());
            var value = properties.getProperty(key);
            if (value == null || value.isBlank() || value.length() > 4096)
                throw new IllegalStateException("秘密后端未提供所需引用。");
            return new SecretValue(value);
        }
    }
}
// Environment 后端按每次解析读取当前 PropertySource，便于开发轮换实验，不缓存密钥。
