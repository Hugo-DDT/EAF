package io.eaf.learning.api;

import java.util.List;

/** 限量规则提议的结果只包含本次来源处理摘要。 */
public record CandidateProposalResult(List<LearningCandidate> candidates, List<CandidateSourceResult> sources) { }
