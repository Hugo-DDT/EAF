package io.eaf.memory.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.eaf.shared.ActorContext;
import java.util.UUID;

/** Learning 只提交已批准修订的内容快照；Memory 在本域验证 Owner、基线并原子发布。 */
public record MemoryCandidateReleaseCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                            int candidateRevision, UUID memoryId, String baseVersion,
                                            JsonNode proposedContent) { }
