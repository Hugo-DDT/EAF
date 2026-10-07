package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record HumanWorkItemPage(List<HumanWorkItem> items, Instant nextCreatedAt, UUID nextId) { }
