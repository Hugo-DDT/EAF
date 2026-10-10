package io.eaf.model.api;

import java.util.UUID;

public record ModelProfileSelection(String selectionKind, UUID assetDefaultProfileId,
                                    ModelProfileSnapshot effectiveProfile) { }
