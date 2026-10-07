package io.eaf.identity.infrastructure;

import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.stereotype.Component;

/** 验证企业 OIDC/JWT Bearer 凭证，并将其稳定主体解析到 EAF 的服务端身份记录。 */
@Component
@ConditionalOnProperty(prefix = "eaf.security", name = "mode", havingValue = "enterprise")
public final class EnterpriseBearerVerifier {
    private static final Set<String> ALLOWED_ALGORITHMS = Set.of("RS256", "RS384", "RS512");
    private final JwtDecoder decoder;
    private final IdentityService identities;

    public EnterpriseBearerVerifier(IdentityService identities, RestTemplateBuilder restTemplateBuilder,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.issuer}") String issuer,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.jwks-uri}") String jwksUri,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.audience}") String audience,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.token-type:at+jwt}") String tokenType,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.jws-algorithm:RS256}") String algorithm,
                                    @org.springframework.beans.factory.annotation.Value("${eaf.security.enterprise.allow-loopback-http:false}") boolean allowLoopbackHttp) {
        this.identities = identities;
        var issuerUri = requireTrustedUri(issuer, "issuer", allowLoopbackHttp);
        var jwks = requireTrustedUri(jwksUri, "jwks-uri", allowLoopbackHttp);
        if (audience == null || audience.isBlank() || audience.length() > 256)
            throw new IllegalArgumentException("企业身份 audience 必须配置且长度不超过 256。");
        if (tokenType == null || tokenType.isBlank() || tokenType.length() > 64)
            throw new IllegalArgumentException("企业身份 token-type 必须配置且长度不超过 64。");
        var configuredAlgorithm = algorithm == null ? "" : algorithm.trim().toUpperCase(java.util.Locale.ROOT);
        if (!ALLOWED_ALGORITHMS.contains(configuredAlgorithm))
            throw new IllegalArgumentException("企业身份签名算法只能使用 RS256、RS384 或 RS512。");

        // JWKS 获取由固定部署配置指向；短超时使密钥服务故障不会长时间挂住入口请求。
        var rest = restTemplateBuilder.connectTimeout(Duration.ofSeconds(2)).readTimeout(Duration.ofSeconds(3)).build();
        var nimbus = NimbusJwtDecoder.withJwkSetUri(jwks.toString())
                .jwsAlgorithm(SignatureAlgorithm.valueOf(configuredAlgorithm))
                .validateType(false)
                .restOperations(rest)
                .build();
        nimbus.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(Duration.ofSeconds(60)),
                new JwtIssuerValidator(issuerUri.toString()),
                EnterpriseBearerVerifier::validateRequiredTimeClaims,
                jwt -> validateAudience(jwt, audience),
                jwt -> validateTokenType(jwt, tokenType)));
        this.decoder = nimbus;
    }

    public Optional<ActorContext> authenticate(String bearerToken) {
        if (bearerToken == null || bearerToken.isBlank()) return Optional.empty();
        try {
            var jwt = decoder.decode(bearerToken);
            // Claims 中的 tenant、role、actor type 均不参与授权，只使用签名主体的 issuer/sub 映射。
            return identities.resolveExternalPrincipal(jwt.getIssuer() == null ? null : jwt.getIssuer().toString(), jwt.getSubject());
        } catch (JwtException | IllegalArgumentException rejected) {
            // 无效 token 与 JWK 获取失败统一 fail closed；不把令牌、签名或上游响应写入日志和 HTTP 错误。
            return Optional.empty();
        }
    }

    private static OAuth2TokenValidatorResult validateAudience(Jwt jwt, String expectedAudience) {
        return jwt.getAudience().contains(expectedAudience) ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "JWT audience is not allowed", null));
    }

    private static OAuth2TokenValidatorResult validateTokenType(Jwt jwt, String expectedType) {
        return expectedType.equals(jwt.getHeaders().get("typ")) ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "JWT type is not allowed", null));
    }

    private static OAuth2TokenValidatorResult validateRequiredTimeClaims(Jwt jwt) {
        return jwt.getExpiresAt() != null && jwt.getNotBefore() != null ? OAuth2TokenValidatorResult.success()
                : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "JWT exp and nbf are required", null));
    }

    private static URI requireTrustedUri(String value, String name, boolean allowLoopbackHttp) {
        final URI uri;
        try {
            uri = URI.create(value == null ? "" : value.trim());
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("企业身份 " + name + " URI 无效。", malformed);
        }
        var scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(java.util.Locale.ROOT);
        var host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(java.util.Locale.ROOT);
        var loopbackHttp = allowLoopbackHttp && "http".equals(scheme)
                && Set.of("127.0.0.1", "::1", "localhost").contains(host);
        if (!uri.isAbsolute() || uri.getUserInfo() != null || uri.getFragment() != null
                || uri.getQuery() != null
                || !"https".equals(scheme) && !loopbackHttp || host.isBlank())
            throw new IllegalArgumentException("企业身份 " + name + " 必须是固定 HTTPS URI；HTTP 仅允许显式 loopback 测试端点。");
        return uri;
    }
}
