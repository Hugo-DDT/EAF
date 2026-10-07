package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.ProtocolVersions;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.springframework.http.MediaType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** MCP 传输层只允许固定版本的 JSON POST 与协议会话关闭，不开启服务器推送；拒绝时直接设置状态码避免 /error 覆盖。 */
@Component
public class McpEndpointValidationFilter extends OncePerRequestFilter {
    private final ObjectMapper json;
    private final boolean enabled;

    public McpEndpointValidationFilter(ObjectMapper json,
                                       @Value("${eaf.agent-protocol.mcp-enabled:true}") boolean enabled) {
        this.json = json;
        this.enabled = enabled;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var contextPath = request.getContextPath();
        // 根上下文以斜杠表示时保留应用路径，避免漏掉 MCP 传输边界校验。
        var path = contextPath.isEmpty() || "/".equals(contextPath)
                ? request.getRequestURI() : request.getRequestURI().substring(contextPath.length());
        if (!"/mcp".equals(path)) {
            chain.doFilter(request, response);
            return;
        }
        // Transport 关闭时隐藏整个 MCP 入口，不能再由残留校验过滤器泄露 406/415 等协议行为。
        if (!enabled) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        if ("GET".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }
        if ("DELETE".equals(request.getMethod())) {
            if (!ProtocolVersions.MCP_2025_11_25.equals(request.getHeader("MCP-Protocol-Version"))) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            chain.doFilter(request, response);
            return;
        }
        if (!"POST".equals(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }
        if (!acceptsMcp(request.getHeader("Accept"))) {
            response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
            return;
        }
        var contentType = request.getContentType();
        try {
            var parsed = contentType == null ? null : MediaType.parseMediaType(contentType);
            if (parsed == null || !MediaType.APPLICATION_JSON.getType().equalsIgnoreCase(parsed.getType())
                    || !MediaType.APPLICATION_JSON.getSubtype().equalsIgnoreCase(parsed.getSubtype())) {
                response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
                return;
            }
        } catch (IllegalArgumentException malformed) {
            response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
            return;
        }
        var bytes = request.getInputStream().readNBytes(McpTransportConfiguration.MAX_REQUEST_BYTES + 1);
        if (bytes.length > McpTransportConfiguration.MAX_REQUEST_BYTES) {
            response.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
            return;
        }
        String method;
        try {
            var message = json.readTree(bytes);
            if (message == null) throw new IOException("MCP JSON-RPC 请求为空。");
            method = message.path("method").asText(null);
        }
        catch (IOException malformed) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        var versionHeader = request.getHeader("MCP-Protocol-Version");
        if (("initialize".equals(method) && versionHeader != null
                && !ProtocolVersions.MCP_2025_11_25.equals(versionHeader))
                || (!"initialize".equals(method)
                && !ProtocolVersions.MCP_2025_11_25.equals(versionHeader))) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        chain.doFilter(new CachedBodyRequest(request, bytes), response);
    }

    private static boolean acceptsMcp(String accept) {
        if (accept == null) return false;
        try {
            var types = MediaType.parseMediaTypes(accept);
            return accepts(types, MediaType.APPLICATION_JSON) && accepts(types, MediaType.TEXT_EVENT_STREAM);
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private static boolean accepts(java.util.List<MediaType> accepted, MediaType expected) {
        return accepted.stream().anyMatch(type -> type.getType().equalsIgnoreCase(expected.getType())
                && type.getSubtype().equalsIgnoreCase(expected.getSubtype()) && type.getQualityValue() > 0);
    }

    private static final class CachedBodyRequest extends HttpServletRequestWrapper {
        private final byte[] body;
        private CachedBodyRequest(HttpServletRequest request, byte[] body) { super(request); this.body = body; }
        @Override public ServletInputStream getInputStream() {
            var input = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override public int read() { return input.read(); }
                @Override public boolean isFinished() { return input.available() == 0; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(ReadListener listener) {
                    try { listener.onDataAvailable(); if (isFinished()) listener.onAllDataRead(); }
                    catch (IOException failure) { listener.onError(failure); }
                }
            };
        }
        @Override public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
