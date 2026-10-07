package io.eaf.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModuleBoundaryTest {
    @Test
    void sharedHasNoDomainDependenciesAndDomainsDoNotDependOnBootstrap() throws IOException {
        var root = projectRoot();
        assertThat(Files.readString(root.resolve("shared/pom.xml")))
                .doesNotContain("<dependency>");
        try (var children = Files.list(root)) {
            children.filter(Files::isDirectory)
                    .map(path -> path.resolve("pom.xml"))
                    .filter(Files::exists)
                    .filter(path -> !path.getParent().getFileName().toString().equals("bootstrap"))
                    .forEach(path -> {
                        try {
                            assertThat(Files.readString(path))
                                    .as(path.toString())
                                    .doesNotContain("<artifactId>eaf-bootstrap</artifactId>");
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
        }
    }

    @Test
    void runtimeUsesExecutionApiInsteadOfConnectorInternals() throws IOException {
        var root = projectRoot();
        var runtimePom = Files.readString(root.resolve("agent-runtime/pom.xml"));
        var executionPom = Files.readString(root.resolve("execution/pom.xml"));
        String runtimeSources;
        try (var sourceFiles = Files.walk(root.resolve("agent-runtime/src/main/java"))) {
            runtimeSources = sourceFiles.filter(Files::isRegularFile)
                    .map(path -> {
                        try { return Files.readString(path); }
                        catch (IOException e) { throw new AssertionError(e); }
                    })
                    .reduce("", String::concat);
        }

        assertThat(runtimePom).contains("<artifactId>eaf-execution</artifactId>")
                .doesNotContain("<artifactId>eaf-connector</artifactId>")
                .doesNotContain("<artifactId>eaf-integration</artifactId>");
        assertThat(executionPom).contains("<artifactId>eaf-connector</artifactId>")
                .doesNotContain("<artifactId>eaf-integration</artifactId>");
        assertThat(runtimeSources).doesNotContain("connector.infrastructure")
                .doesNotContain("integration.infrastructure")
                .doesNotContain("task.task");
    }

    private Path projectRoot() throws IOException {
        var path = Path.of("").toAbsolutePath();
        while (path != null && (!Files.exists(path.resolve("pom.xml"))
                || !Files.readString(path.resolve("pom.xml")).contains("<module>shared</module>"))) {
            path = path.getParent();
        }
        if (path == null) throw new AssertionError("project root not found");
        return path;
    }
}
// 本文件负责实现 EAF 的 ModuleBoundaryTest.java 相关代码。
