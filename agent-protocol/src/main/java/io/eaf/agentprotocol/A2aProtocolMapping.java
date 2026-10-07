package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.agent.api.RemoteAgentCatalog;
import io.eaf.agent.api.RemoteAgentRegistration;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.ConnectorService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskStatus;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.web.util.UriComponentsBuilder;
import org.a2aproject.sdk.spec.AgentCard;
import org.a2aproject.sdk.spec.AgentCapabilities;
import org.a2aproject.sdk.spec.AgentInterface;
import org.a2aproject.sdk.spec.AgentSkill;
import org.a2aproject.sdk.spec.Artifact;
import org.a2aproject.sdk.spec.DataPart;
import org.a2aproject.sdk.spec.Task;
import org.a2aproject.sdk.spec.TaskState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** 将 EAF 已授权 Agent/Task 事实映射为固定 A2A 1.0 描述；不执行远端调用。 */
@Service
public class A2aProtocolMapping {
    private static final Set<String> MESSAGE_FIELDS = Set.of("messageId", "role", "parts");
    private static final UUID P12_RESULT_SYNC_CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000011");
    private final RemoteAgentCatalog remoteAgents;
    private final CapabilityService capabilities;
    private final ConnectorService connectors;
    private final ObjectMapper json;
    private final String endpoint;
    private final String cardVersion;

    public A2aProtocolMapping(RemoteAgentCatalog remoteAgents, CapabilityService capabilities,
                              ConnectorService connectors, ObjectMapper json,
                              @Value("${eaf.protocol.a2a.endpoint:http://127.0.0.1:18080/a2a}") String endpoint,
                              @Value("${eaf.protocol.a2a.card-version:0.1.0}") String cardVersion) {
        this.remoteAgents = remoteAgents;
        this.capabilities = capabilities;
        this.connectors = connectors;
        this.json = json;
        this.endpoint = endpoint;
        this.cardVersion = cardVersion;
    }

    public RegisteredReviewer requireRegisteredReviewer(ActorContext actor, UUID workspaceId) {
        var registration = remoteAgents.requireActive(actor, workspaceId, "risk-review");
        // 登记中的版本和 Connector ID 是固定引用；读取时仍通过各自公开 API 检查当前可用性。
        var capability = capabilities.requirePublished(actor, workspaceId,
                registration.capabilityId(), registration.capabilityVersion());
        var connector = connectors.requireActive(actor.tenantId(), workspaceId, "A2A_REVIEW_PEER");
        if (!connector.id().equals(registration.connectorId())
                || !connector.allowedUses().containsAll(Set.of("a2a.send", "a2a.get"))
                || !connector.permissions().containsAll(registration.delegationActions()))
            throw EafException.notFound();
        return new RegisteredReviewer(registration, capability, connector);
    }

    public AgentCard card(ActorContext actor, UUID workspaceId) {
        // Capability 列表只含当前可见的已发布版本；AgentSkill 仅用于发现，不授予执行权。
        // 固定结果同步只能由业务 Workflow 发起，不列入通用 A2A 能力发现。
        var skills = capabilities.list(actor, workspaceId).stream()
                .filter(capability -> !P12_RESULT_SYNC_CAPABILITY_ID.equals(capability.id()))
                .map(capability -> new AgentSkill(capability.name() + "@" + capability.version(),
                        capability.name(), capability.description(), List.of("capability"), List.of(),
                        List.of("application/json"), List.of("application/json"), List.of()))
                .toList();
        return card(skills, UriComponentsBuilder.fromUriString(endpoint).pathSegment(workspaceId.toString())
                .build().toUriString());
    }

    public AgentCard publicCard() {
        // 未认证发现只返回固定服务描述，不泄露租户、Workspace 或业务 Capability。
        return card(List.of(), endpoint);
    }

    public JsonNode cardJson(ActorContext actor, UUID workspaceId) {
        // 返回前按 A2A schema 序列化模型，不把 Java SDK 类型直接交给 MVC 猜测转换器。
        return json.valueToTree(card(actor, workspaceId));
    }

    public JsonNode publicCardJson() {
        return json.valueToTree(publicCard());
    }

    private AgentCard card(List<AgentSkill> skills, String interfaceUrl) {
        var interfaceSpec = new AgentInterface("JSONRPC", interfaceUrl, null, "1.0");
        return AgentCard.builder().name("Enterprise AI Fabric")
                .description("受企业身份与 Workspace 授权治理的 Agent 服务。")
                .version(cardVersion)
                .capabilities(AgentCapabilities.builder().streaming(false).pushNotifications(false)
                        .extendedAgentCard(false).build())
                .defaultInputModes(List.of("application/json"))
                .defaultOutputModes(List.of("application/json"))
                .skills(skills).supportedInterfaces(List.of(interfaceSpec)).build();
    }

    public InboundMessage parseUserMessage(JsonNode message) {
        if (message == null || !message.isObject() || hasUnknownFields(message, MESSAGE_FIELDS))
            throw EafException.invalid("A2A Message 包含未支持字段。");
        var messageId = message.path("messageId");
        if (!messageId.isTextual() || messageId.asText().isBlank() || messageId.asText().length() > 128)
            throw EafException.invalid("A2A messageId 无效。");
        if (!"ROLE_USER".equals(message.path("role").asText()))
            throw EafException.invalid("A2A Message 只接受 ROLE_USER。");
        var parts = message.path("parts");
        if (!parts.isArray() || parts.size() != 1 || !parts.get(0).isObject()
                || hasUnknownFields(parts.get(0), Set.of("text")))
            throw EafException.invalid("A2A Message 只接受一个纯文本 Part。");
        var text = parts.get(0).path("text");
        if (!text.isTextual() || text.asText().isBlank() || text.asText().length() > 8000)
            throw EafException.invalid("A2A Message 文本无效。");
        return new InboundMessage(messageId.asText(), text.asText());
    }

    public Task task(TaskResponse response) {
        return task(response, true);
    }

    public Task task(TaskResponse response, boolean includeArtifacts) {
        var artifacts = !includeArtifacts || response.result() == null ? List.<Artifact>of() : List.of(Artifact.builder()
                .artifactId(response.id() + ":result").name("result").description("受控 Task 结果")
                .parts(List.of(new DataPart(json.convertValue(response.result(), Object.class)))).build());
        var metadata = response.errorCode() == null ? Map.<String, Object>of()
                : Map.<String, Object>of("eaf.errorCode", response.errorCode());
        // A2A ID 是 EAF Task 的关联标识；读取授权始终由认证身份、Workspace 和 Task API 决定。
        return new Task(response.id().toString(), response.rootTaskId().toString(),
                new org.a2aproject.sdk.spec.TaskStatus(state(response.status()), null,
                        response.updatedAt().atOffset(ZoneOffset.UTC)),
                artifacts, List.of(), metadata);
    }

    public JsonNode taskJson(TaskResponse response, boolean includeArtifacts) {
        var result = json.valueToTree(task(response, includeArtifacts));
        // A2A 1.0 的 ListTasks 默认省略 artifacts 字段，避免额外返回业务结果。
        if (!includeArtifacts && result instanceof ObjectNode object) object.remove("artifacts");
        return result;
    }

    private static boolean hasUnknownFields(JsonNode node, Set<String> allowed) {
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) if (!allowed.contains(fields.next())) return true;
        return false;
    }

    private static TaskState state(String status) {
        return switch (TaskStatus.valueOf(status)) {
            case QUEUED -> TaskState.TASK_STATE_SUBMITTED;
            // A2A 只把本地远端等待投影为 WORKING；公开状态不泄露 peer 的具体错误或连接信息。
            case RUNNING, WAITING_VERIFICATION, WAITING_REMOTE, CANCELLING_REMOTE -> TaskState.TASK_STATE_WORKING;
            case WAITING_APPROVAL -> TaskState.TASK_STATE_INPUT_REQUIRED;
            case SUCCEEDED -> TaskState.TASK_STATE_COMPLETED;
            case FAILED, TIMED_OUT -> TaskState.TASK_STATE_FAILED;
            case CANCELLED -> TaskState.TASK_STATE_CANCELED;
        };
    }

    public record InboundMessage(String messageId, String text) { }

    public record RegisteredReviewer(RemoteAgentRegistration registration, CapabilityDefinition capability,
                                     ConnectorDefinition connector) { }
}
