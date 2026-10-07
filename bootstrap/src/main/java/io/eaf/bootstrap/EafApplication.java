package io.eaf.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import io.eaf.integration.infrastructure.TestCrmHttpIntegration;
import java.time.Clock;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(scanBasePackages = "io.eaf", exclude = UserDetailsServiceAutoConfiguration.class)
@EnableScheduling
@Import(TestCrmHttpIntegration.class)
public class EafApplication {

    public static void main(String[] args) {
        if (java.util.Arrays.stream(args).anyMatch(arg -> arg.startsWith("--eaf.p20.maintenance-command="))) {
            var maintenance = new SpringApplication(P20MaintenanceConfiguration.class);
            maintenance.setAdditionalProfiles("p20-maintenance");
            maintenance.setWebApplicationType(WebApplicationType.NONE);
            try (ConfigurableApplicationContext context = maintenance.run(args)) {
                SpringApplication.exit(context);
            }
            return;
        }
        SpringApplication.run(EafApplication.class, args);
    }

    @Bean
    Clock clock() { return Clock.systemUTC(); }
}
// 本文件负责实现 EAF 的 EafApplication.java 相关代码。
