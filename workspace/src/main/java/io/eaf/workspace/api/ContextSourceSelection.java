package io.eaf.workspace.api;

import java.util.List;
import java.util.UUID;

public record ContextSourceSelection(List<ContextSourceSummary> sources,
                                     List<UUID> selectedSourceWorkspaceIds,
                                     int unavailableSourceCount, Integer nextOffset) { }
