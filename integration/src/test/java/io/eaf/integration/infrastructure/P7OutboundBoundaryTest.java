package io.eaf.integration.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.RemoteA2aOutcome;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.credential.api.ResolvedCredential;
import io.eaf.shared.EafException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// /19 验证企业目标边界、重定向/响应上限，以及 CRM 写入契约的幂等冲突。
class P7OutboundBoundaryTest {
    private static final String TOKEN = "synthetic-outbound-token";

    @Test
    void arbitraryA2aTargetIsRejectedBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var adapter = new A2aHttpIntegration(new ObjectMapper(), credentials, new OutboundTargetPolicy("enterprise", true, ""));

        var result = adapter.sendTask(a2a("http://169.254.169.254:80/latest/meta-data"), "rpc-1", "agent.risk.review",
                "message-1", "synthetic request", Instant.now().plusSeconds(2));

        assertThat(result.outcome()).isEqualTo(RemoteA2aOutcome.REJECTED);
        assertThat(result.errorCode()).isEqualTo("CONNECTOR_UNAVAILABLE");
        verify(credentials, never()).resolve(any());
    }

    @Test
    void enterpriseA2aRequiresExactTargetAndMatchingResolvedAddress() {
        var credentials = mock(CredentialResolutionPort.class);
        var policy = new OutboundTargetPolicy("enterprise", true,
                "https://localhost:443/a2a=192.0.2.17");
        var adapter = new A2aHttpIntegration(new ObjectMapper(), credentials, policy);

        var result = adapter.sendTask(a2a("https://localhost:443/a2a"), "rpc-2", "agent.risk.review",
                "message-2", "synthetic request", Instant.now().plusSeconds(2));

        assertThat(result.outcome()).isEqualTo(RemoteA2aOutcome.REJECTED);
        assertThat(result.errorCode()).isEqualTo("CONNECTOR_UNAVAILABLE");
        verify(credentials, never()).resolve(any());
    }

    @Test
    void a2aRedirectIsReturnedWithoutForwardingCredentialToRedirectTarget() throws Exception {
        var redirectTargetCalls = new AtomicInteger();
        var receivedAuthorization = new AtomicReference<String>();
        var redirectTarget = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        redirectTarget.createContext("/capture", exchange -> {
            redirectTargetCalls.incrementAndGet();
            exchange.close();
        });
        redirectTarget.start();
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/a2a", exchange -> {
            receivedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            var target = "http://127.0.0.1:" + redirectTarget.getAddress().getPort() + "/capture";
            exchange.getResponseHeaders().add("Location", target);
            exchange.sendResponseHeaders(307, -1);
            exchange.close();
        });
        peer.start();
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolve(any())).thenReturn(new ResolvedCredential("peer-fixture", 1, TOKEN));
        var adapter = new A2aHttpIntegration(new ObjectMapper(), credentials, new OutboundTargetPolicy("local", false, ""));
        try {
            var result = adapter.sendTask(a2a("http://127.0.0.1:" + peer.getAddress().getPort() + "/a2a"), "rpc-3",
                    "agent.risk.review", "message-3", "synthetic request", Instant.now().plusSeconds(3));
            assertThat(result.outcome()).isEqualTo(RemoteA2aOutcome.REJECTED);
            assertThat(result.errorCode()).isEqualTo("A2A_HTTP_307");
            assertThat(receivedAuthorization).hasValue("Bearer " + TOKEN);
            assertThat(redirectTargetCalls).hasValue(0);
        } finally {
            peer.stop(0);
            redirectTarget.stop(0);
        }
    }

    @Test
    void testCrmRejectsEnterpriseUseBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var adapter = new TestCrmHttpIntegration(new ObjectMapper(), credentials,
                new OutboundTargetPolicy("enterprise", false, ""));

        assertThatThrownBy(() -> adapter.readCustomer(crm("http://127.0.0.1:18081"), "customer-1", Instant.now().plusSeconds(2)))
                .isInstanceOfSatisfying(EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("CONNECTOR_UNAVAILABLE"));
        verify(credentials, never()).resolve(any());
    }

    @Test
    // 企业模式下连契约夹具也不能解析凭据，避免把合成 CRM 伪装成真实服务。
    void crmContractFixtureRejectsEnterpriseUseBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var adapter = new TestCrmHttpIntegration(new ObjectMapper(), credentials,
                new OutboundTargetPolicy("enterprise", false, ""));

        assertThatThrownBy(() -> adapter.readCustomer(contractCrm("http://127.0.0.1:19092"),
                "customer-1", Instant.now().plusSeconds(2)))
                .isInstanceOfSatisfying(EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("CONNECTOR_UNAVAILABLE"));
        verify(credentials, never()).resolve(any());
    }

    @Test
    void testCrmStopsReadingAfterConfiguredResponseLimit() throws Exception {
        var crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var response = "x".repeat(16_385).getBytes(StandardCharsets.UTF_8);
        crm.createContext("/customers/customer-1", exchange -> {
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        crm.start();
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolve(any())).thenReturn(new ResolvedCredential("crm-fixture", 1, TOKEN));
        var adapter = new TestCrmHttpIntegration(new ObjectMapper(), credentials,
                new OutboundTargetPolicy("local", false, ""));
        try {
            assertThatThrownBy(() -> adapter.readCustomer(crm("http://127.0.0.1:" + crm.getAddress().getPort()),
                    "customer-1", Instant.now().plusSeconds(3)))
                    .isInstanceOfSatisfying(EafException.class,
                            failure -> assertThat(failure.code()).isEqualTo("INVALID_TOOL_RESULT"));
        } finally {
            crm.stop(0);
        }
    }

    @Test
    void oversizedTestCrmWriteIsRejectedBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var adapter = new TestCrmHttpIntegration(new ObjectMapper(), credentials,
                new OutboundTargetPolicy("local", false, ""));
        var result = adapter.createFollowup(crm("http://127.0.0.1:18082"), "operation-1", "customer-1",
                "x".repeat(20_000), "owner-1", Instant.now().plusSeconds(2));

        assertThat(result.state()).isEqualTo("REJECTED");
        assertThat(result.errorCode()).isEqualTo("CRM_REQUEST_TOO_LARGE");
        verify(credentials, never()).resolve(any());
    }

    @Test
    void p7CrmWriteContractUsesStableIdempotencyKeyAndRejectsChangedArguments() throws Exception {
        var records = new ConcurrentHashMap<String, String>();
        var posts = new AtomicInteger();
        var reads = new AtomicInteger();
        var crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var json = new ObjectMapper();
        crm.createContext("/followups", exchange -> {
            posts.incrementAndGet();
            var request = json.readTree(exchange.getRequestBody());
            var operationId = request.path("operationId").asText();
            if (!"POST".equals(exchange.getRequestMethod())
                    || !operationId.equals(exchange.getRequestHeaders().getFirst("Idempotency-Key"))
                    || !"EAF-CRM-WRITE-V1".equals(request.path("contractVersion").asText())) {
                exchange.sendResponseHeaders(400, -1);
                exchange.close();
                return;
            }
            var response = json.createObjectNode().put("contractVersion", "EAF-CRM-WRITE-V1")
                    .put("operationId", operationId).put("externalId", "fu-" + operationId)
                    .put("customerId", request.path("customerId").asText())
                    .put("summary", request.path("summary").asText())
                    .put("ownerId", request.path("ownerId").asText())
                    .put("status", "CREATED").put("acceptedAt", "2026-10-01T00:00:00Z");
            var body = json.writeValueAsString(response);
            var existing = records.putIfAbsent(operationId, body);
            if (existing != null && !existing.equals(body)) {
                exchange.sendResponseHeaders(409, -1);
                exchange.close();
                return;
            }
            var bytes = (existing == null ? body : existing).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        crm.createContext("/followups/by-operation/", exchange -> {
            reads.incrementAndGet();
            var operationId = exchange.getRequestURI().getPath().substring("/followups/by-operation/".length());
            var body = records.get(operationId);
            if (body == null) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        crm.start();
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolve(any())).thenReturn(new ResolvedCredential("p7-crm-write", 1, TOKEN));
        var adapter = new TestCrmHttpIntegration(json, credentials, new OutboundTargetPolicy("local", false, ""));
        var connector = p7WriteCrm("http://127.0.0.1:" + crm.getAddress().getPort());
        try {
            var created = adapter.createFollowup(connector, "stable-operation-19", "customer-1", "原始摘要",
                    "owner-1", Instant.now().plusSeconds(3));
            var conflict = adapter.createFollowup(connector, "stable-operation-19", "customer-1", "异参摘要",
                    "owner-1", Instant.now().plusSeconds(3));
            var readback = adapter.findFollowup(connector, "stable-operation-19", Instant.now().plusSeconds(3));

            assertThat(created.state()).isEqualTo("ACCEPTED");
            assertThat(conflict.state()).isEqualTo("REJECTED");
            assertThat(conflict.errorCode()).isEqualTo("CRM_IDEMPOTENCY_CONFLICT");
            assertThat(readback).isPresent();
            assertThat(readback.orElseThrow().summary()).isEqualTo("原始摘要");
            assertThat(posts).hasValue(2);
            assertThat(reads).hasValue(1);
            assertThat(records).hasSize(1);
        } finally {
            crm.stop(0);
        }
    }

    private ConnectorDefinition a2a(String baseUrl) {
        return new ConnectorDefinition(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "A2A_REVIEW_PEER",
                baseUrl, "ACTIVE", "peer-fixture", "eaf:a2a:peer", Set.of("a2a.send"), Set.of("agent:risk-review"));
    }

    private ConnectorDefinition crm(String baseUrl) {
        return new ConnectorDefinition(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "TEST_CRM",
                baseUrl, "ACTIVE", "crm-fixture", "eaf:test-crm", Set.of("customer.read"), Set.of("crm.customer.read"));
    }

    private ConnectorDefinition contractCrm(String baseUrl) {
        return new ConnectorDefinition(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "P7_CRM_READ_CONTRACT_FIXTURE",
                baseUrl, "ACTIVE", "p7-crm-read", "eaf:p7-crm-read", Set.of("customer.read"), Set.of("crm.customer.read"));
    }

    private ConnectorDefinition p7WriteCrm(String baseUrl) {
        return new ConnectorDefinition(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "P7_CRM_WRITE_CONTRACT_FIXTURE",
                baseUrl, "ACTIVE", "p7-crm-write", "eaf:p7-crm-write",
                Set.of("customer.read", "followup.create", "followup.verify"),
                Set.of("crm.customer.read", "crm.followup.create", "crm.followup.read"));
    }
}
