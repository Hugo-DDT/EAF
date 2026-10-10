package io.eaf.model.api;

import java.util.List;
import java.util.UUID;

public interface ModelProfileCatalog {
    List<ModelProfileAvailability> listSelectable(UUID assetDefaultProfileId);
    ModelProfileSelection resolveForTask(UUID assetDefaultProfileId, ModelProfileRef requested);
    ModelProfileSnapshot requireCurrent(ModelProfileSnapshot frozen);
    ModelBillingProfile billingIdentity(ModelProfileSnapshot profile);
}
