package io.eaf.integration.infrastructure;

import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.shared.EafException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 将连接器登记目标约束为本地 fixture 或显式登记的企业 HTTPS peer。 */
@Component
public final class OutboundTargetPolicy {
    private final boolean enterprise;
    private final boolean egressPolicyConfirmed;
    private final Map<String, Set<String>> enterpriseTargets;

    public OutboundTargetPolicy(@Value("${eaf.security.mode:disabled}") String securityMode,
                                @Value("${eaf.outbound.enterprise.egress-policy-confirmed:false}") boolean egressPolicyConfirmed,
                                @Value("${eaf.outbound.a2a.enterprise-targets:}") String enterpriseTargets) {
        this.enterprise = "enterprise".equalsIgnoreCase(securityMode);
        this.egressPolicyConfirmed = egressPolicyConfirmed;
        this.enterpriseTargets = parseEnterpriseTargets(enterpriseTargets);
    }

    /** A2A 企业 peer 必须命中完整 URI 与 DNS 地址清单；本地 fixture 仅使用显式 loopback。 */
    public URI a2aEndpoint(ConnectorDefinition connector) {
        var uri = parse(connector == null ? null : connector.baseUrl(), "A2A Connector URL 无效。");
        if (enterprise) {
            var configuredAddresses = enterpriseTargets.get(uri.toASCIIString());
            if (!egressPolicyConfirmed || configuredAddresses == null || !validEnterpriseUri(uri)
                    || !resolvesOnlyTo(uri.getHost(), configuredAddresses))
                throw unavailable("A2A 企业目标未登记、DNS 地址不符或部署出口策略未确认。");
            return uri;
        }
        if (!validLoopbackUri(uri)) throw unavailable("本地 A2A 仅允许显式 127.0.0.1 HTTP fixture。");
        return uri;
    }

    /** 当前 CRM 实现只服务合成 loopback fixture，不允许在 enterprise 模式冒充真实 CRM。 */
    public URI testCrmEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "测试 CRM 目标登记无效。",
                "测试 CRM 仅允许非 enterprise 模式下的显式 loopback fixture。", Set.of("TEST_CRM"));
    }

    /** CRM 契约夹具也只允许非 enterprise loopback，不能充当真实企业 CRM。 */
    public URI crmContractFixtureEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "CRM 契约夹具目标登记无效。",
                "CRM 契约夹具仅允许非 enterprise 模式下的显式 loopback fixture。",
                Set.of("P7_CRM_READ_CONTRACT_FIXTURE", "P7_CRM_WRITE_CONTRACT_FIXTURE"));
    }

    /** 结果 fixture 单独使用 loopback Connector 与结果专用凭据，不改写既有写入快照。 */
    public URI crmOutcomeFixtureEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "CRM 结果 fixture 目标无效。",
                "CRM 结果 fixture 仅允许非 enterprise 模式下的显式 loopback 目标。",
                Set.of("P12_CRM_OUTCOME_FIXTURE"));
    }

    /** 服务台只允许非 enterprise 模式下显式登记的 loopback 合成目标。 */
    public URI serviceRequestFixtureEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "服务台 fixture 目标无效。",
                "服务台 fixture 仅允许非 enterprise 模式下的显式 loopback 目标。",
                Set.of("P15_INTERNAL_SERVICE_DESK_FIXTURE"));
    }

    public URI p27OaFixtureEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "OA fixture 目标登记无效。",
                "P27 OA 只允许非 enterprise 模式下的显式 loopback fixture。", Set.of("P27_OA_TODO_FIXTURE"));
    }

    public URI p27ServiceDeskFixtureEndpoint(ConnectorDefinition connector, String suffix) {
        return loopbackFixtureEndpoint(connector, suffix, "P27 服务台 fixture 目标登记无效。",
                "P27 服务台只允许非 enterprise 模式下的显式 loopback fixture。",
                Set.of("P27_SERVICE_DESK_RESULT_FIXTURE"));
    }

    private URI loopbackFixtureEndpoint(ConnectorDefinition connector, String suffix, String invalidTargetMessage,
            String rejectedTargetMessage, Set<String> providers) {
        var base = parse(connector == null ? null : connector.baseUrl(), invalidTargetMessage);
        if (enterprise || connector == null || !providers.contains(connector.provider())
                || !validLoopbackUri(base) || (base.getRawPath() != null && !base.getRawPath().isEmpty()
                && !"/".equals(base.getRawPath())))
            throw unavailable(rejectedTargetMessage);
        return URI.create(base.toString().replaceAll("/$", "") + suffix);
    }

    private Map<String, Set<String>> parseEnterpriseTargets(String value) {
        if (value == null || value.isBlank()) return Map.of();
        try {
            return Arrays.stream(value.split(";"))
                    .map(String::trim).filter(item -> !item.isEmpty())
                    .map(item -> {
                        int split = item.indexOf('=');
                        if (split < 1 || split == item.length() - 1) throw new IllegalArgumentException();
                        var target = item.substring(0, split).trim();
                        var uri = URI.create(target);
                        if (!validEnterpriseUri(uri) || !target.equals(uri.toASCIIString())) throw new IllegalArgumentException();
                        var addresses = Arrays.stream(item.substring(split + 1).split(","))
                                .map(String::trim).filter(address -> !address.isEmpty())
                                .map(this::numericAddressKey).collect(Collectors.toUnmodifiableSet());
                        if (addresses.isEmpty()) throw new IllegalArgumentException();
                        return Map.entry(target, addresses);
                    })
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException("A2A enterprise target must use exact https://host:443/path=ip[,ip][;...] entries.", invalid);
        }
    }

    private boolean resolvesOnlyTo(String host, Set<String> allowedAddresses) {
        try {
            var resolved = InetAddress.getAllByName(host);
            return resolved.length > 0 && Arrays.stream(resolved)
                    .allMatch(address -> allowedAddresses.contains(addressKey(address.getAddress())));
        } catch (Exception resolutionFailure) {
            return false;
        }
    }

    private String numericAddressKey(String address) {
        // 清单只接受数字字面量，避免配置解析本身再触发一次 DNS 查询。
        boolean ipv4 = address.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}");
        boolean ipv6 = address.matches("[0-9a-fA-F:]+") && address.contains(":");
        if (!ipv4 && !ipv6) throw new IllegalArgumentException("Enterprise address must be an IP literal.");
        try { return addressKey(InetAddress.getByName(address).getAddress()); }
        catch (Exception invalid) { throw new IllegalArgumentException("Enterprise address is invalid.", invalid); }
    }

    private String addressKey(byte[] address) { return HexFormat.of().formatHex(address); }

    private boolean validEnterpriseUri(URI uri) {
        return uri != null && "https".equals(uri.getScheme()) && uri.getHost() != null
                && uri.getPort() == 443 && !isIpLiteral(uri.getHost()) && uri.getRawUserInfo() == null
                && uri.getRawQuery() == null && uri.getRawFragment() == null
                && uri.getRawPath() != null && !uri.getRawPath().isBlank() && !uri.getRawPath().endsWith("/");
    }

    private boolean validLoopbackUri(URI uri) {
        return uri != null && "http".equals(uri.getScheme()) && "127.0.0.1".equals(uri.getHost())
                && uri.getPort() > 0 && uri.getPort() <= 65535 && uri.getRawUserInfo() == null
                && uri.getRawQuery() == null && uri.getRawFragment() == null;
    }

    private boolean isIpLiteral(String host) {
        return host.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}") || host.contains(":");
    }

    private URI parse(String value, String message) {
        try { return URI.create(value); }
        catch (RuntimeException invalid) { throw EafException.conflict("CONNECTOR_UNAVAILABLE", message); }
    }

    private EafException unavailable(String message) {
        return EafException.conflict("CONNECTOR_UNAVAILABLE", message);
    }
}
// 本文件负责企业 A2A 出站目标登记、DNS 预检以及合成 CRM loopback 范围。
