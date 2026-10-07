package io.eaf.bootstrap;

import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskExecutionService;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "eaf.security.mode=local",
        "eaf.task.dispatcher-enabled=false",
        "eaf.task.max-concurrent=2",
        "eaf.task.max-queued=4",
        "eaf.task.evaluation-slot-wait=PT5S",
        "eaf.task.shutdown-grace=PT2S",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false"
})
class P19BoundedExecutionTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
            Set.of("task:create", "task:read", "task:cancel", "evaluation:run"));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
    }

    @Autowired TaskService tasks;
    @Autowired TaskExecutionService execution;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean TaskRunner runner;

    @Test
    void queueAdmissionAndUserEvaluationShareOnlyTwoRealExecutionSlots() throws Exception {
        var user1 = create("p19-user-1", "USER");
        var user2 = create("p19-user-2", "USER");
        var user3 = create("p19-user-3", "USER");
        var evaluation = create("p19-evaluation", "EVALUATION");
        assertThatThrownBy(() -> create("p19-overflow", "USER"))
                .isInstanceOf(EafException.class)
                .satisfies(failure -> assertThat(((EafException) failure).code()).isEqualTo("TASK_CAPACITY_EXCEEDED"));
        assertThat(create("p19-user-1", "USER").id()).isEqualTo(user1.id());
        assertThat(jdbc.queryForObject("select count(*) from task.task where status = 'QUEUED'", Long.class)).isEqualTo(4L);

        var usersEntered = new CountDownLatch(2);
        var evaluationEntered = new CountDownLatch(1);
        var releaseUsers = new CountDownLatch(1);
        var releaseEvaluation = new CountDownLatch(1);
        var current = new AtomicInteger();
        var peak = new AtomicInteger();
        doAnswer(invocation -> {
            var work = invocation.getArgument(0, io.eaf.task.api.TaskWorkItem.class);
            var active = current.incrementAndGet();
            peak.accumulateAndGet(active, Math::max);
            try {
                if ("EVALUATION".equals(work.source())) {
                    evaluationEntered.countDown();
                    if (!releaseEvaluation.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("evaluation barrier timed out");
                } else {
                    usersEntered.countDown();
                    if (!releaseUsers.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("user barrier timed out");
                }
                return TaskRunner.RunOutcome.success("{}", false, null, null);
            } finally {
                current.decrementAndGet();
            }
        }).when(runner).run(any());

        try (var callers = Executors.newSingleThreadExecutor()) {
            assertThat(execution.dispatchNext()).isTrue();
            assertThat(execution.dispatchNext()).isTrue();
            assertThat(usersEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(execution.dispatchNext()).isFalse();
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, user3.id()).status()).isEqualTo(TaskStatus.QUEUED);

            var evaluationResult = callers.submit(() -> execution.executeEvaluation(ALICE, Ids.WORKSPACE_A, evaluation.id()));
            assertThat(evaluationEntered.await(200, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, evaluation.id()).status()).isEqualTo(TaskStatus.QUEUED);
            releaseUsers.countDown();
            assertThat(evaluationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(peak.get()).isEqualTo(2);
            assertThat(jdbc.queryForObject("select count(*) from task.task where status = 'RUNNING'", Long.class)).isEqualTo(1L);

            releaseEvaluation.countDown();
            assertThat(evaluationResult.get(5, TimeUnit.SECONDS).status()).isEqualTo(TaskStatus.SUCCEEDED);
        } finally {
            releaseUsers.countDown();
            releaseEvaluation.countDown();
        }
    }

    private io.eaf.task.api.TaskSnapshot create(String key, String source) {
        return tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "1.0.0",
                "concurrency fixture " + key, null, null, key, "trace-" + key, source));
    }
}
