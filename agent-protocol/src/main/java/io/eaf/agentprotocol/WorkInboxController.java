package io.eaf.agentprotocol;

import io.eaf.approval.api.ApprovalService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.task.api.TaskService;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}")
public class WorkInboxController {
    private static final Set<String> KINDS = Set.of("HUMAN", "APPROVAL", "TASK_RESULT", "BRIEF");
    private final WorkflowService workflows;
    private final ApprovalService approvals;
    private final TaskService tasks;
    private final WorkspaceAuthorization workspaces;

    public WorkInboxController(WorkflowService workflows, ApprovalService approvals, TaskService tasks,
            WorkspaceAuthorization workspaces) {
        this.workflows = workflows;
        this.approvals = approvals;
        this.tasks = tasks;
        this.workspaces = workspaces;
    }

    @GetMapping("/work-inbox")
    InboxPage inbox(@PathVariable UUID workspaceId, @RequestParam MultiValueMap<String, String> query,
            Authentication authentication) {
        if (query.keySet().stream().anyMatch(key -> !Set.of("kind", "cursor", "limit").contains(key)))
            throw EafException.invalid("收件箱查询包含未允许字段。");
        var kind = single(query, "kind");
        kind = kind == null ? "HUMAN" : kind.toUpperCase(java.util.Locale.ROOT);
        if (!KINDS.contains(kind)) throw EafException.invalid("收件箱 kind 只允许 HUMAN、APPROVAL、TASK_RESULT、BRIEF。");
        var actor = ApiSupport.actor(authentication);
        if (actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("员工收件箱只接受直接 HUMAN 身份。");
        var limit = integer(single(query, "limit"), 20);
        if (limit < 1 || limit > 50) throw EafException.invalid("收件箱 limit 必须在 1 到 50 之间。");
        var cursor = decode(kind, single(query, "cursor"));
        if (!authorized(actor, workspaceId, kind)) return new InboxPage(kind, List.of(), null, false, "PERMISSION_REQUIRED");
        return switch (kind) {
            case "HUMAN" -> human(actor, workspaceId, cursor, limit);
            case "APPROVAL" -> approval(actor, workspaceId, cursor, limit);
            case "TASK_RESULT" -> taskResults(actor, workspaceId, cursor, limit);
            case "BRIEF" -> briefs(actor, workspaceId, cursor, limit);
            default -> throw EafException.invalid("收件箱 kind 无效。");
        };
    }

    private InboxPage human(ActorContext actor, UUID workspaceId, Cursor cursor, int limit) {
        var page = workflows.listMyHumanInbox(actor, workspaceId, cursor == null ? null : cursor.time(),
                cursor == null ? null : cursor.id(), limit);
        var items = page.items().stream().map(item -> new Item("HUMAN", item.id(), item.title(), item.ownerId(),
                item.assigneeId(), item.state(), item.waitingReason(), item.updatedAt(), null, item.detailRef(),
                item.allowedActions(), item.blocked())).toList();
        return new InboxPage("HUMAN", items, encode("HUMAN", page.nextUpdatedAt(), page.nextId()), true, null);
    }

    private InboxPage approval(ActorContext actor, UUID workspaceId, Cursor cursor, int limit) {
        var page = approvals.listActionablePending(actor, workspaceId, cursor == null ? null : cursor.time(),
                cursor == null ? null : cursor.id(), limit);
        var items = page.items().stream().map(item -> new Item("APPROVAL", item.id(), "待处理审批", null, actor.actorId(),
                "PENDING", null, item.createdAt(), item.taskId(), "/api/v1/workspaces/" + workspaceId
                + "/approvals/" + item.id(), Set.of("read", "decide"), false)).toList();
        return new InboxPage("APPROVAL", items, encode("APPROVAL", page.nextCreatedAt(), page.nextId()), true, null);
    }

    private InboxPage taskResults(ActorContext actor, UUID workspaceId, Cursor cursor, int limit) {
        var page = tasks.listMyRootResults(actor, workspaceId, cursor == null ? null : cursor.time(),
                cursor == null ? null : cursor.id(), limit);
        var items = page.items().stream().map(item -> new Item("TASK_RESULT", item.id(), "任务结果", actor.actorId(), null,
                item.status(), null, item.updatedAt(), item.id(), "/api/v1/workspaces/" + workspaceId
                + "/tasks/" + item.id(), Set.of("read"), false)).toList();
        return new InboxPage("TASK_RESULT", items, encode("TASK_RESULT", page.nextUpdatedAt(), page.nextId()), true, null);
    }

    private InboxPage briefs(ActorContext actor, UUID workspaceId, Cursor cursor, int limit) {
        var page = workflows.listProjectBriefInbox(actor, workspaceId, cursor == null ? null : cursor.time(),
                cursor == null ? null : cursor.id(), limit);
        var items = page.items().stream().map(item -> new Item("BRIEF", item.id(), item.title(), item.ownerId(), null,
                item.state(), item.waitingReason(), item.updatedAt(), null, item.detailRef(), item.allowedActions(), item.blocked())).toList();
        return new InboxPage("BRIEF", items, encode("BRIEF", page.nextUpdatedAt(), page.nextId()), true, null);
    }

    private boolean authorized(ActorContext actor, UUID workspaceId, String kind) {
        var actions = switch (kind) {
            case "HUMAN" -> List.of("work-item:read");
            case "APPROVAL" -> List.of("approval:read", "approval:decide");
            case "TASK_RESULT" -> List.of("task:read");
            case "BRIEF" -> List.of("workflow:read");
            default -> List.<String>of();
        };
        return actions.stream().allMatch(action -> workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, action));
    }

    private static String single(MultiValueMap<String, String> query, String key) {
        var values = query.get(key);
        if (values == null) return null;
        if (values.size() != 1) throw EafException.invalid(key + " 只能提供一次。");
        return values.getFirst();
    }

    private static int integer(String raw, int fallback) {
        if (raw == null) return fallback;
        try { return Integer.parseInt(raw); }
        catch (NumberFormatException invalid) { throw EafException.invalid("limit 必须是整数。"); }
    }

    private static String encode(String kind, Instant time, UUID id) {
        if (time == null || id == null) return null;
        var payload = kind + "\n" + time + "\n" + id;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }

    private static Cursor decode(String kind, String encoded) {
        if (encoded == null) return null;
        try {
            var fields = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8).split("\\n", -1);
            if (fields.length != 3 || !kind.equals(fields[0])) throw new IllegalArgumentException();
            return new Cursor(Instant.parse(fields[1]), UUID.fromString(fields[2]));
        } catch (RuntimeException invalid) {
            throw EafException.invalid("收件箱 cursor 无效或属于其他 kind。");
        }
    }

    private record Cursor(Instant time, UUID id) { }
    record InboxPage(String kind, List<Item> items, String nextCursor, boolean available, String reason) {
        InboxPage { items = List.copyOf(items); }
    }
    record Item(String kind, UUID id, String title, UUID ownerId, UUID assigneeId, String state, String waitingReason,
            Instant updatedAt, UUID taskId, String detailRef, Set<String> allowedActions, boolean blocked) {
        Item { allowedActions = Set.copyOf(allowedActions); }
    }
}
