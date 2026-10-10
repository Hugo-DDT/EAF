package io.eaf.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModuleBoundaryTest {
    private static final Pattern IMPORT = Pattern.compile("^\\s*import\\s+(?:static\\s+)?([^;]+);");
    private static final Pattern PACKAGE = Pattern.compile("^\\s*package\\s+([^;]+);");
    private static final Map<String, String> DOMAIN_SQL_OWNERS = Map.of(
            "task.task", "task",
            "evaluation.candidate_context_snapshot", "evaluation",
            "workspace.\"grant\"", "workspace");

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

    @Test
    void productionImportsUsePublicModuleApis() throws IOException {
        var root = projectRoot();
        var packages = new HashMap<String, String>();
        var sources = new ArrayList<SourceFile>();
        try (var modules = Files.list(root)) {
            for (var module : modules.filter(Files::isDirectory).filter(path -> Files.exists(path.resolve("pom.xml"))).toList()) {
                var sourceRoot = module.resolve("src/main/java");
                if (!Files.isDirectory(sourceRoot)) continue;
                try (var files = Files.walk(sourceRoot)) {
                    for (var file : files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java")).toList()) {
                        var content = Files.readString(file);
                        var packageLine = content.lines().map(PACKAGE::matcher).filter(java.util.regex.Matcher::find)
                                .map(matcher -> matcher.group(1)).findFirst().orElse("");
                        if (packageLine.isEmpty()) continue;
                        packages.put(packageLine, module.getFileName().toString());
                        sources.add(new SourceFile(module.getFileName().toString(), file, content));
                    }
                }
            }
        }

        var violations = new ArrayList<String>();
        for (var source : sources) {
            for (var line : source.content().lines().toList()) {
                var matcher = IMPORT.matcher(line);
                if (!matcher.matches()) continue;
                var imported = matcher.group(1).replaceAll("\\.\\*$", "");
                if (!imported.startsWith("io.eaf.")) continue;
                var owner = packages.entrySet().stream()
                        .filter(entry -> imported.equals(entry.getKey()) || imported.startsWith(entry.getKey() + "."))
                        .max(java.util.Comparator.comparingInt(entry -> entry.getKey().length()))
                        .map(Map.Entry::getValue).orElse(null);
                if (owner != null && !allowedImport(source.module(), owner, imported))
                    violations.add(source.path() + " imports " + imported + " from " + owner);
            }
        }
        assertThat(violations).as("cross-module production imports must use public APIs").isEmpty();
    }

    @Test
    void boundaryRuleAllowsApiAndSharedButRejectsInfrastructureImports() {
        assertThat(allowedImport("task", "evaluation", "io.eaf.evaluation.api.ScenarioEvaluationService")).isTrue();
        assertThat(allowedImport("task", "shared", "io.eaf.shared.ActorContext")).isTrue();
        assertThat(allowedImport("task", "evaluation", "io.eaf.evaluation.infrastructure.JdbcScenarioEvaluationService")).isFalse();
        assertThat(allowedImport("bootstrap", "evaluation", "io.eaf.evaluation.infrastructure.JdbcScenarioEvaluationService")).isTrue();
    }

    @Test
    void productionSqlReferencesStayInsideTheirOwningModules() throws IOException {
        var root = projectRoot();
        var sources = new ArrayList<SourceFile>();
        try (var modules = Files.list(root)) {
            for (var module : modules.filter(Files::isDirectory).filter(path -> Files.exists(path.resolve("pom.xml"))).toList()) {
                var sourceRoot = module.resolve("src/main/java");
                if (!Files.isDirectory(sourceRoot)) continue;
                try (var files = Files.walk(sourceRoot)) {
                    files.filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".java"))
                            .forEach(path -> {
                                try { sources.add(new SourceFile(module.getFileName().toString(), path, Files.readString(path))); }
                                catch (IOException e) { throw new AssertionError(e); }
                            });
                }
            }
        }
        var violations = sources.stream().flatMap(source -> knownDomainSqlViolations(source.module(), source.content()).stream()
                .map(table -> source.path() + " references " + table + " owned by " + DOMAIN_SQL_OWNERS.get(table))).toList();
        assertThat(violations).as("known domain SQL must stay in its owning module").isEmpty();
    }

    @Test
    void knownDomainSqlRuleAllowsOwnersAndRejectsCrossModuleReferences() {
        assertThat(knownDomainSqlViolations("task", "select * from task.task")).isEmpty();
        assertThat(knownDomainSqlViolations("evaluation", "select * from evaluation.candidate_context_snapshot")).isEmpty();
        assertThat(knownDomainSqlViolations("workspace", "select * from workspace.\"grant\"")).isEmpty();
        assertThat(knownDomainSqlViolations("workflow", "select * from task.task")).containsExactly("task.task");
        assertThat(knownDomainSqlViolations("learning", "select * from evaluation.candidate_context_snapshot"))
                .containsExactly("evaluation.candidate_context_snapshot");
        assertThat(knownDomainSqlViolations("workflow", "select * from workspace.\"grant\"")).containsExactly("workspace.\"grant\"");
    }

    private boolean allowedImport(String source, String target, String imported) {
        return source.equals(target) || "shared".equals(target) || "bootstrap".equals(source)
                || imported.matches("io\\.eaf\\.[^.]+\\.api(?:\\..*)?");
    }

    private List<String> knownDomainSqlViolations(String module, String source) {
        return DOMAIN_SQL_OWNERS.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(module))
                .filter(entry -> Pattern.compile("(?i)\\b(?:from|join|update|into)\\s+"
                                + Pattern.quote(entry.getKey()) + "(?=\\s|$|\\(|;)")
                        .matcher(source).find())
                .map(Map.Entry::getKey).toList();
    }

    private record SourceFile(String module, Path path, String content) { }

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
