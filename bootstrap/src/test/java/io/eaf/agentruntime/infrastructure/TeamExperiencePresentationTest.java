package io.eaf.agentruntime.infrastructure;

import io.eaf.context.api.ContextItem;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.model.api.ModelMessage;
import io.eaf.shared.Hashing;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TeamExperiencePresentationTest {
    @Test
    void exactDedupKeepsFirstOrderAndEveryCitationWhileBaselinePreservesDuplicates() {
        var first = "团队经验：先检查进纸组件\n适用条件：清理后仍卡纸\n建议：记录卡纸位置并安排维护复查。";
        var differentCondition = "团队经验：先检查进纸组件\n适用条件：新纸盒装入后立即卡纸\n建议：记录卡纸位置并安排维护复查。";
        var context = new EnterpriseContext("READY", 3, 2_000, 0, 0, null, List.of(
                item("team-experience-1", first), item("team-experience-2", first),
                item("team-experience-3", differentCondition)));

        var baseline = TeamExperiencePresentation.render(context, TeamExperiencePresentation.Strategy.VERBATIM);
        var candidate = TeamExperiencePresentation.render(context, TeamExperiencePresentation.Strategy.EXACT_DEDUP);

        assertThat(baseline.messageText()).isEqualTo(TeamExperiencePresentation.PREFIX
                + first + "\n\n" + first + "\n\n" + differentCondition);
        assertThat(candidate.messageText()).isEqualTo(TeamExperiencePresentation.PREFIX
                + first + "\n\n" + differentCondition);
        assertThat(candidate.groups()).extracting(TeamExperiencePresentation.Group::citationIds)
                .containsExactly(List.of("team-experience-1", "team-experience-2"),
                        List.of("team-experience-3"));
        assertThat(candidate.sourceCount()).isEqualTo(3);
        assertThat(candidate.originalChars()).isGreaterThan(candidate.renderedChars());
        assertThat(candidate.originalUtf8Bytes()).isGreaterThan(candidate.renderedUtf8Bytes());
        var baselineRequest = List.of(new ModelMessage("system", "固定提示词"),
                new ModelMessage("user", baseline.messageText() + "\n用户任务：固定合成简报"));
        var candidateRequest = List.of(new ModelMessage("system", "固定提示词"),
                new ModelMessage("user", candidate.messageText() + "\n用户任务：固定合成简报"));
        var baselineChars = baselineRequest.stream().mapToInt(message -> message.content().length()).sum();
        var candidateChars = candidateRequest.stream().mapToInt(message -> message.content().length()).sum();
        assertThat(baselineChars - candidateChars).isEqualTo(baseline.originalChars() - candidate.renderedChars());
        var baselineBytes = baselineRequest.stream().mapToInt(message ->
                message.content().getBytes(java.nio.charset.StandardCharsets.UTF_8).length).sum();
        var candidateBytes = candidateRequest.stream().mapToInt(message ->
                message.content().getBytes(java.nio.charset.StandardCharsets.UTF_8).length).sum();
        assertThat(baselineBytes - candidateBytes)
                .isEqualTo(baseline.originalUtf8Bytes() - candidate.renderedUtf8Bytes());
        assertThat(Hashing.sha256(baselineRequest.toString())).isNotEqualTo(Hashing.sha256(candidateRequest.toString()));
    }

    @Test
    void noDuplicateMeansIdenticalBaselineAndCandidateMessages() {
        var context = new EnterpriseContext("READY", 2, 2_000, 0, 0, null, List.of(
                item("team-experience-1", "经验 A"), item("team-experience-2", "经验 B")));

        assertThat(TeamExperiencePresentation.render(context, TeamExperiencePresentation.Strategy.EXACT_DEDUP).messageText())
                .isEqualTo(TeamExperiencePresentation.render(context, TeamExperiencePresentation.Strategy.VERBATIM).messageText());
    }

    private static ContextItem item(String citationId, String content) {
        return new ContextItem(citationId, "MEMORY", null, 0, null, null, null, "1.0.0", null,
                "source-hash-" + citationId, content, null, 0, "TEAM", List.of(), null,
                "SERVICE_REQUEST", "printer-jam");
    }
}
