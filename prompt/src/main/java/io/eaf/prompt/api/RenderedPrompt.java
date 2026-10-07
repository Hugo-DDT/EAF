package io.eaf.prompt.api;

import java.util.List;

public record RenderedPrompt(String version, List<Message> messages) {
    public record Message(String role, String content) { }
}
// 本文件负责实现 EAF 的 RenderedPrompt.java 相关代码。
