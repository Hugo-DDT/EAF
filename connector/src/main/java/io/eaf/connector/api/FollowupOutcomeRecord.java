package io.eaf.connector.api;

import java.time.Instant;
import java.util.UUID;

/** CRM 中追加保存的业务结果回执；与原跟进 CREATED 回执保持独立。 */
public record FollowupOutcomeRecord(String operationId, String externalId, String customerId,
                                    UUID followupId, UUID resultId, int resultNo, UUID recordedBy,
                                    String outcomeCode, String summary, String nextAction,
                                    Instant nextContactAt, String disposition, String status,
                                    Instant acceptedAt) { }
