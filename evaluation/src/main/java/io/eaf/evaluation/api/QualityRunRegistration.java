package io.eaf.evaluation.api;

import java.time.Instant;
import java.util.UUID;

/** Evaluation 服务端创建的用途/来源关联；该 ID 才能进入带标记的 USER/Workflow Task。 */
public record QualityRunRegistration(UUID id, String purpose, String source, String status,
                                     Instant createdAt) { }
