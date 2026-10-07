package io.eaf.skill.api;

import java.util.List;
import java.util.UUID;

// 每个版本固定自己的输入输出契约、Prompt、Tool 和评测引用。
public record CreateSkillVersionCommand(String version, String inputSchema, String outputSchema,
                                        UUID promptId, String promptVersion,
                                        List<SkillToolReference> toolDependencies,
                                        String evaluationRef) { }
