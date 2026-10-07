package io.eaf.bootstrap;

import io.eaf.identity.infrastructure.IdentityAuthenticationFilter;
import io.eaf.agentprotocol.McpEndpointValidationFilter;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.web.access.intercept.AuthorizationFilter;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration(proxyBeanMethods = false)
class SecurityConfiguration {

    @Bean
    SecurityFilterChain healthOnly(HttpSecurity http, IdentityAuthenticationFilter authenticationFilter,
                                   McpEndpointValidationFilter mcpValidationFilter,
                                   Environment environment) throws Exception {
        return http
                .authorizeHttpRequests(requests -> {
                        requests
                        .requestMatchers("/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness").permitAll()
                        .requestMatchers("/.well-known/agent-card.json").permitAll()
                        // 聚合指标只允许经 Identity 映射的 SERVICE 抓取主体读取。
                        .requestMatchers("/actuator/prometheus").access((authentication, context) -> {
                            var current = authentication.get();
                            var principal = current == null ? null : current.getPrincipal();
                            return new AuthorizationDecision(current != null && current.isAuthenticated()
                                    && principal instanceof ActorContext actor && actor.type() == ActorType.SERVICE);
                        });
                        if (environment.acceptsProfiles(Profiles.of("local")))
                            requests.requestMatchers("/demo", "/demo/**").permitAll();
                // MCP/A2A 传输入口使用同一身份认证过滤器，业务控制器不信任请求体声明的调用者。
                        requests.requestMatchers("/api/**", "/mcp", "/mcp/**", "/a2a", "/a2a/**").authenticated()
                                .anyRequest().denyAll();
                })
                .addFilterBefore(authenticationFilter, UsernamePasswordAuthenticationFilter.class)
                .addFilterAfter(mcpValidationFilter, AuthorizationFilter.class)
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint((request, response, exception) ->
                                response.setStatus(isProtectedApiRequest(request) ? 401 : 403))
                        .accessDeniedHandler((request, response, exception) ->
                                response.setStatus(isProtectedApiRequest(request) && isAnonymousRequest()
                                        ? 401 : 403)))
                .httpBasic(basic -> basic.disable())
                .formLogin(form -> form.disable())
                .logout(logout -> logout.disable())
                .requestCache(cache -> cache.disable())
                .csrf(csrf -> csrf.disable())
                .build();
    }

    private static boolean isProtectedApiRequest(HttpServletRequest request) {
        // 直接设置状态可避免 /error 再分派覆盖认证结果；根上下文路径不能截掉 MCP 入口首字符。
        var contextPath = request.getContextPath();
        var path = contextPath.isEmpty() || "/".equals(contextPath)
                ? request.getRequestURI() : request.getRequestURI().substring(contextPath.length());
        return path.startsWith("/api/") || path.equals("/mcp") || path.startsWith("/mcp/")
                || path.equals("/a2a") || path.startsWith("/a2a/")
                || path.equals("/actuator/prometheus")
                || request.getServletPath().equals("/mcp");
    }

    private static boolean isAnonymousRequest() {
        // AnonymousAuthenticationToken 也标记为 authenticated，不能用普通布尔状态判断匿名访问。
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication == null || !authentication.isAuthenticated()
                || authentication instanceof AnonymousAuthenticationToken;
    }

    @Bean
    FilterRegistrationBean<McpEndpointValidationFilter> disableGlobalMcpFilterRegistration(
            McpEndpointValidationFilter filter) {
        var registration = new FilterRegistrationBean<>(filter);
        registration.setEnabled(false);
        return registration;
    }
}
// 本文件负责实现 EAF 的 SecurityConfiguration.java 相关代码。
