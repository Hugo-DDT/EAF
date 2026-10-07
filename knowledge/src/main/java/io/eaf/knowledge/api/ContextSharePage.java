package io.eaf.knowledge.api;

import java.util.List;

public record ContextSharePage(List<ContextShare> items, Integer nextOffset) { }
