package io.eaf.agentprotocol;

import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.McpTransportContextExtractor;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityException;
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.ProtocolVersions;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** MCP 只接入受保护的 HTTP 传输和已存在的 Task/Capability 应用入口。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "eaf.agent-protocol.mcp-enabled", havingValue = "true", matchIfMissing = true)
public class McpTransportConfiguration {
    static final String ACTOR_CONTEXT_KEY = "eaf.actor";
    static final int MAX_REQUEST_BYTES = 32 * 1024;

    @Bean
    HttpServletStreamableServerTransportProvider mcpServletTransport() {
        return HttpServletStreamableServerTransportProvider.builder()
                .jsonMapper(McpJsonDefaults.getMapper())
                .mcpEndpoint("/mcp")
                .contextExtractor(McpTransportConfiguration::transportContext)
                .securityValidator(McpTransportConfiguration::validateOrigin)
                .maxRequestSize(MAX_REQUEST_BYTES)
                .build();
    }

    @Bean
    McpStreamableServerTransportProvider mcpVersionPinnedTransport(
            HttpServletStreamableServerTransportProvider transport) {
        // SDK 默认会协商多个历史版本；此 adapter 只对外声明锁定的版本。
        return new McpStreamableServerTransportProvider() {
            @Override public void setSessionFactory(io.modelcontextprotocol.spec.McpStreamableServerSession.Factory factory) {
                transport.setSessionFactory(factory);
            }
            @Override public reactor.core.publisher.Mono<Void> notifyClients(String method, Object params) {
                return transport.notifyClients(method, params);
            }
            @Override public reactor.core.publisher.Mono<Void> notifyClient(String sessionId, String method, Object params) {
                return transport.notifyClient(sessionId, method, params);
            }
            @Override public reactor.core.publisher.Mono<Void> closeGracefully() { return transport.closeGracefully(); }
            @Override public void close() { transport.close(); }
            @Override public List<String> protocolVersions() { return List.of(ProtocolVersions.MCP_2025_11_25); }
        };
    }

    @Bean
    ServletRegistrationBean<?> mcpServlet(HttpServletStreamableServerTransportProvider transport) {
        var registration = new ServletRegistrationBean<>(transport, "/mcp");
        registration.setAsyncSupported(true);
        return registration;
    }

    @Bean(destroyMethod = "close")
    McpSyncServer mcpServer(@Qualifier("mcpVersionPinnedTransport") McpStreamableServerTransportProvider transport,
                            McpTaskTools tools) {
        return McpServer.sync(transport)
                .serverInfo("eaf", "0.1.0")
                .capabilities(ServerCapabilities.builder().tools(false).build())
                .strictToolNameValidation(false)
                .validateToolInputs(true)
                .tools(tools.specifications())
                .build();
    }

    private static McpTransportContext transportContext(HttpServletRequest request) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.isAuthenticated()
                && authentication.getPrincipal() instanceof io.eaf.shared.ActorContext actor)
            return McpTransportContext.create(Map.of(ACTOR_CONTEXT_KEY, actor));
        return McpTransportContext.EMPTY;
    }

    private static void validateOrigin(Map<String, List<String>> headers) throws ServerTransportSecurityException {
        var origins = header(headers, "Origin");
        if (origins.isEmpty()) return;
        if (origins.size() != 1 || !isLoopbackOrigin(origins.getFirst()))
            throw new ServerTransportSecurityException(403, "MCP Origin 仅允许本地来源。");
    }

    private static List<String> header(Map<String, List<String>> headers, String name) {
        return headers.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .flatMap(entry -> entry.getValue().stream()).toList();
    }

    private static boolean isLoopbackOrigin(String origin) {
        try {
            var uri = URI.create(origin);
            var host = uri.getHost();
            return ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    && host != null && (host.equalsIgnoreCase("localhost") || host.equals("127.0.0.1")
                    || host.equals("::1") || host.equals("[::1]"))
                    && uri.getRawUserInfo() == null && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
                    && uri.getRawQuery() == null && uri.getRawFragment() == null;
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }
}
