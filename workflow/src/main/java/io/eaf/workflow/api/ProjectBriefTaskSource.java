package io.eaf.workflow.api;

import java.util.UUID;

public record ProjectBriefTaskSource(UUID briefId, String evidenceBundleJson, String evidenceBundleHash,
        boolean generationAttempted, String preparedResultJson) { }
