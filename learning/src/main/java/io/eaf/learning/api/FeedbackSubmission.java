package io.eaf.learning.api;

/** 标记本次命令是否新建反馈，供 REST 适配器返回 201 或幂等重放的 200。 */
public record FeedbackSubmission(Feedback feedback, boolean created) { }
