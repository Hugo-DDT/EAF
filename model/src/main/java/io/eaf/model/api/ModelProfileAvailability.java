package io.eaf.model.api;

public record ModelProfileAvailability(ModelProfileSnapshot profile, boolean selectable, String reason) { }
