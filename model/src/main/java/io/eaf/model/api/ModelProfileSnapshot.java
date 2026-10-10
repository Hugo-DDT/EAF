package io.eaf.model.api;

import java.util.UUID;

/** 非秘密、不可变的模型配置事实；实际授权仍由 Gateway 在出站时复核。 */
public record ModelProfileSnapshot(UUID profileId, String version, String configurationHash,
                                   int schemaVersion, String displayName, String mode, String provider,
                                   String requestedModel, String adapterId, String adapterVersion,
                                   String connectionConfigurationHash, double temperature,
                                   int maxInputBudget, int maxOutputTokens, String responseFormat,
                                   boolean toolCallingEnabled, String retryPolicy,
                                   String billingProvider, String billingModel, String callType,
                                   String pricingPolicy) { }
