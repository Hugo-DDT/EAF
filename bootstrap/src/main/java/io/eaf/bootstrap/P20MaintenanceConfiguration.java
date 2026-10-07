package io.eaf.bootstrap;

import io.eaf.model.infrastructure.ProviderSharedQuota;
import io.eaf.task.infrastructure.TaskClusterCapacityMaintenance;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/** 离线维护上下文只装配数据库 Owner，不启动 Web、Dispatcher 或业务调度。 */
@Configuration(proxyBeanMethods = false)
@Profile("p20-maintenance")
@EnableAutoConfiguration
@Import({TaskClusterCapacityMaintenance.class, ProviderSharedQuota.class, P20MaintenanceRunner.class})
public class P20MaintenanceConfiguration { }
