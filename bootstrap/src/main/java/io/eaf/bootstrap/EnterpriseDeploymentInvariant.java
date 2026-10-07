package io.eaf.bootstrap;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** 检查企业部署 Profile 未被高优先级配置降级，且运行数据库角色不兼任迁移角色。 */
@Component
public final class EnterpriseDeploymentInvariant implements SmartInitializingSingleton {
    private final Environment environment;
    private final String securityMode;
    private final String secretBackend;
    private final String runtimeDatabaseUser;
    private final String migrationDatabaseUser;

    public EnterpriseDeploymentInvariant(Environment environment,
                                         @Value("${eaf.security.mode:disabled}") String securityMode,
                                         @Value("${eaf.secrets.backend:unconfigured}") String secretBackend,
                                         @Value("${spring.datasource.username:}") String runtimeDatabaseUser,
                                         @Value("${spring.flyway.user:}") String migrationDatabaseUser) {
        this.environment = environment;
        this.securityMode = securityMode;
        this.secretBackend = secretBackend;
        this.runtimeDatabaseUser = runtimeDatabaseUser;
        this.migrationDatabaseUser = migrationDatabaseUser;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!environment.acceptsProfiles(org.springframework.core.env.Profiles.of("enterprise"))) return;
        // 入口校验部署覆盖后的最终值；企业 Profile 不得因环境变量覆盖而开启 local alias。
        if (!"enterprise".equalsIgnoreCase(securityMode))
            throw new IllegalStateException("enterprise Profile 必须使用 enterprise 身份模式。");
        if ("environment".equalsIgnoreCase(secretBackend))
            throw new IllegalStateException("enterprise Profile 不允许使用 Environment 秘密后端。");
        if (runtimeDatabaseUser == null || runtimeDatabaseUser.isBlank()
                || migrationDatabaseUser == null || migrationDatabaseUser.isBlank()
                || runtimeDatabaseUser.equalsIgnoreCase(migrationDatabaseUser))
            throw new IllegalStateException("enterprise Profile 必须使用彼此独立的运行与 Flyway 数据库用户。");
    }
}
