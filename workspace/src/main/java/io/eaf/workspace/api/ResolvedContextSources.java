package io.eaf.workspace.api;

import java.util.List;

public record ResolvedContextSources(List<WorkspaceSummary> sources, int unavailableSourceCount) { }
