package io.eaf.identity.infrastructure;

import io.eaf.identity.api.IdentityService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class IdentityAuthenticationFilter extends OncePerRequestFilter {
    private final IdentityService identities;
    private final ObjectProvider<EnterpriseBearerVerifier> enterpriseVerifiers;
    private final String mode;

    public IdentityAuthenticationFilter(IdentityService identities,
                                        ObjectProvider<EnterpriseBearerVerifier> enterpriseVerifiers,
                                         @Value("${eaf.security.mode:disabled}") String mode) {
        this.identities = identities;
        this.enterpriseVerifiers = enterpriseVerifiers;
        this.mode = mode;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if ("local".equals(mode)) {
            var header = request.getHeader("Authorization");
            if (header != null && header.startsWith("Bearer ")) {
                var token = header.substring(7).trim();
                var delegationHeader = request.getHeader("X-EAF-Delegation");
                var resolved = delegationHeader == null || delegationHeader.isBlank()
                        ? identities.resolveToken(token).filter(actor -> actor.type() != io.eaf.shared.ActorType.AGENT)
                        : resolveDelegated(token, delegationHeader);
                resolved.ifPresent(actor ->
                        SecurityContextHolder.getContext().setAuthentication(
                                new UsernamePasswordAuthenticationToken(actor, null, java.util.List.of())));
            }
        } else if ("enterprise".equals(mode)) {
            // 企业模式只接受一个标准 Bearer 头；多头或格式歧义 fail closed。
            var token = bearerToken(request);
            var verifier = enterpriseVerifiers.getIfAvailable();
            if (token.isPresent() && verifier != null)
                verifier.authenticate(token.get()).ifPresent(actor -> SecurityContextHolder.getContext().setAuthentication(
                        new UsernamePasswordAuthenticationToken(actor, null, java.util.List.of())));
        }
        chain.doFilter(request, response);
    }

    private static java.util.Optional<String> bearerToken(HttpServletRequest request) {
        var headers = request.getHeaders("Authorization");
        if (headers == null || !headers.hasMoreElements()) return java.util.Optional.empty();
        var header = headers.nextElement();
        if (headers.hasMoreElements() || header == null || header.length() <= 7
                || !header.regionMatches(true, 0, "Bearer ", 0, 7)) return java.util.Optional.empty();
        var token = header.substring(7).trim();
        if (token.isEmpty() || token.chars().anyMatch(Character::isWhitespace)) return java.util.Optional.empty();
        return java.util.Optional.of(token);
    }

    private java.util.Optional<io.eaf.shared.ActorContext> resolveDelegated(String token, String delegationHeader) {
        try {
            return identities.resolveDelegatedToken(token, UUID.fromString(delegationHeader), IdentityService.REST_AUDIENCE);
        } catch (IllegalArgumentException malformed) {
            return java.util.Optional.empty();
        }
    }
}
// 本文件负责实现 EAF 的 IdentityAuthenticationFilter.java 相关代码。
