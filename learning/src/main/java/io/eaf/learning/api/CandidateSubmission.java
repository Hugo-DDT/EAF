package io.eaf.learning.api;

/** 候选命令重放时返回原记录，并明确本次是否新建。 */
public record CandidateSubmission(LearningCandidate candidate, boolean created) { }
