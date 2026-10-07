package io.eaf.workflow.infrastructure;

import java.util.UUID;

record ParallelBranchIntent(String role, String stepId, String dispatchKey, String inputJson, String inputHash,
                            UUID capabilityId, String capabilityVersion) { }
