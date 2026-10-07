package io.eaf.workflow.api;

import java.util.UUID;

/** Workflow 冻结的团队经验版本引用；正文仍由 Context/Memory 在运行前解析。 */
public record TeamExperienceRef(UUID cardId, int revision) { }
