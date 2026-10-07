package io.eaf.workspace.api;

import java.util.List;

public record WorkspacePage(List<WorkspaceSummary> items, Integer nextOffset) { }
