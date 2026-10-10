package io.eaf.model.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelProfileAvailability;
import io.eaf.model.api.ModelProfileCatalog;
import io.eaf.model.api.ModelProfileRef;
import io.eaf.model.api.ModelProfileSelection;
import io.eaf.model.api.ModelProfileSnapshot;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/** model 域持有两个固定 P15 档位；版本定义写入后不覆盖。 */
@Service
public final class JdbcModelProfileCatalog implements ModelProfileCatalog {
    public static final UUID DEFAULT_PROFILE_ID = UUID.fromString("22000000-0000-4000-8000-000000000001");
    public static final UUID BOUNDED_PROFILE_ID = UUID.fromString("22000000-0000-4000-8000-000000000002");
    public static final String VERSION = "1.0.0";

    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final ModelGateway gateway;
    private final String mode;
    private final String provider;
    private final String liveModel;
    private final String baseUrl;

    public JdbcModelProfileCatalog(JdbcTemplate jdbc, ObjectMapper json, ModelGateway gateway,
                                   @Value("${eaf.model.mode:deterministic}") String mode,
                                   @Value("${eaf.model.live.provider:${EAF_MODEL_LIVE_PROVIDER:dashscope}}") String provider,
                                   @Value("${eaf.model.live.model:${EAF_MODEL_LIVE_MODEL:}}") String liveModel,
                                   @Value("${eaf.model.live.base-url:${EAF_MODEL_LIVE_BASE_URL:}}") String baseUrl) {
        this.jdbc = jdbc;
        this.json = json;
        this.gateway = gateway;
        this.mode = mode;
        this.provider = provider;
        this.liveModel = liveModel;
        this.baseUrl = baseUrl;
        registerIfAbsent(current(DEFAULT_PROFILE_ID, "默认输出", 8_000));
        registerIfAbsent(current(BOUNDED_PROFILE_ID, "有界输出", 1_024));
    }

    @Override
    public List<ModelProfileAvailability> listSelectable(UUID assetDefaultProfileId) {
        if (!DEFAULT_PROFILE_ID.equals(assetDefaultProfileId)) return List.of();
        return List.of(DEFAULT_PROFILE_ID, BOUNDED_PROFILE_ID).stream().map(this::availability).toList();
    }

    @Override
    public ModelProfileSelection resolveForTask(UUID assetDefaultProfileId, ModelProfileRef requested) {
        if (!DEFAULT_PROFILE_ID.equals(assetDefaultProfileId))
            throw EafException.conflict("MODEL_PROFILE_CONFIGURATION_UNAVAILABLE", "固定 P15 Agent 默认档位未登记。 ");
        var ref = requested == null ? new ModelProfileRef(assetDefaultProfileId, VERSION) : requested;
        if (!DEFAULT_PROFILE_ID.equals(ref.profileId()) && !BOUNDED_PROFILE_ID.equals(ref.profileId()))
            throw EafException.invalid("只能选择目录中的固定 P15 模型档位。 ");
        if (!VERSION.equals(ref.version())) throw EafException.conflict("MODEL_PROFILE_NOT_CURRENT", "模型档位版本不可用。 ");
        var profile = registered(ref.profileId(), ref.version());
        requireCurrent(profile);
        return new ModelProfileSelection(requested == null ? "DEFAULT" : "EXPLICIT", assetDefaultProfileId, profile);
    }

    @Override
    public ModelProfileSnapshot requireCurrent(ModelProfileSnapshot frozen) {
        if (frozen == null) throw EafException.conflict("MODEL_PROFILE_NOT_CURRENT", "任务没有冻结模型档位。 ");
        var row = jdbc.query("select configuration_hash, snapshot::text, status from model.profile_version where profile_id = ? and version = ?",
                rs -> rs.next() ? new ProfileRow(rs.getString("configuration_hash"), read(rs.getString("snapshot")), rs.getString("status")) : null,
                frozen.profileId(), frozen.version());
        if (row == null || !"ACTIVE".equals(row.status()))
            throw EafException.conflict("MODEL_PROFILE_DISABLED", "模型档位已禁用或不存在。 ");
        if (!row.hash().equals(frozen.configurationHash()) || !row.hash().equals(currentHash(frozen.profileId())))
            throw EafException.conflict("MODEL_PROFILE_CONFIGURATION_CHANGED", "模型档位配置与冻结版本不一致。 ");
        if (!gatewayAvailable(row.snapshot()))
            throw EafException.conflict("MODEL_PROFILE_CONFIGURATION_UNAVAILABLE", "当前模型 Gateway 不支持该档位。 ");
        return row.snapshot();
    }

    @Override
    public ModelBillingProfile billingIdentity(ModelProfileSnapshot profile) {
        if (profile == null || profile.billingProvider() == null || profile.billingModel() == null) return null;
        return new ModelBillingProfile(profile.billingProvider(), profile.billingModel(), profile.callType());
    }

    private ModelProfileAvailability availability(UUID id) {
        var profile = registered(id, VERSION);
        var status = jdbc.queryForObject("select status from model.profile_version where profile_id = ? and version = ?",
                String.class, id, VERSION);
        if (!"ACTIVE".equals(status)) return new ModelProfileAvailability(profile, false, "MODEL_PROFILE_DISABLED");
        if (!profile.configurationHash().equals(currentHash(id)))
            return new ModelProfileAvailability(profile, false, "MODEL_PROFILE_CONFIGURATION_CHANGED");
        return gatewayAvailable(profile) ? new ModelProfileAvailability(profile, true, null)
                : new ModelProfileAvailability(profile, false, "MODEL_PROFILE_CONFIGURATION_UNAVAILABLE");
    }

    private ModelProfileSnapshot registered(UUID id, String version) {
        return jdbc.query("select snapshot::text from model.profile_version where profile_id = ? and version = ?",
                rs -> rs.next() ? read(rs.getString(1)) : null, id, version);
    }

    private ModelProfileSnapshot current(UUID id, String displayName, int maxOutputTokens) {
        boolean live = "live-model".equalsIgnoreCase(mode);
        var currentBilling = gateway.billingProfile();
        var actualProvider = live ? provider.toLowerCase(java.util.Locale.ROOT) : "deterministic";
        var actualModel = live ? liveModel : "p15-deterministic-v1";
        var adapter = !live ? "deterministic" : "deepseek".equalsIgnoreCase(provider)
                ? "spring-ai-openai-compatible" : "spring-ai-alibaba";
        var adapterVersion = !live ? "1" : "1.1.2";
        var targetHash = Hashing.sha256(actualProvider + "\u001f" + String.valueOf(baseUrl) + "\u001f" + adapter);
        var billingProvider = currentBilling == null ? null : currentBilling.provider();
        var billingModel = currentBilling == null ? null : currentBilling.model();
        var callType = currentBilling == null ? null : currentBilling.callType();
        var modeName = live ? "LIVE" : "DETERMINISTIC";
        var responseFormat = "JSON_OBJECT";
        var retryPolicy = "NONE";
        var raw = String.join("\u001f", VERSION, modeName, actualProvider, actualModel, adapter, adapterVersion,
                targetHash, "0.0", "8000", String.valueOf(maxOutputTokens), responseFormat, "false", retryPolicy,
                String.valueOf(billingProvider), String.valueOf(billingModel), String.valueOf(callType),
                billingProvider == null ? "NOT_APPLICABLE" : "CURRENT_AT_CALL");
        return new ModelProfileSnapshot(id, VERSION, Hashing.sha256(raw), 1, displayName, modeName,
                actualProvider, actualModel, adapter, adapterVersion, targetHash, 0.0d, 8_000,
                maxOutputTokens, responseFormat, false, retryPolicy, billingProvider, billingModel,
                callType == null ? "CHAT" : callType, billingProvider == null ? "NOT_APPLICABLE" : "CURRENT_AT_CALL");
    }

    private String currentHash(UUID id) {
        return current(id, id.equals(DEFAULT_PROFILE_ID) ? "默认输出" : "有界输出",
                id.equals(DEFAULT_PROFILE_ID) ? 8_000 : 1_024).configurationHash();
    }

    private boolean gatewayAvailable(ModelProfileSnapshot profile) {
        if ("DETERMINISTIC".equals(profile.mode())) return true;
        var billing = gateway.billingProfile();
        return billing != null && profile.provider().equalsIgnoreCase(billing.provider())
                && profile.requestedModel().equals(billing.model())
                && java.util.Objects.equals(profile.billingProvider(), billing.provider())
                && java.util.Objects.equals(profile.billingModel(), billing.model());
    }

    private void registerIfAbsent(ModelProfileSnapshot profile) {
        var snapshot = jdbc.queryForObject("select count(*) from model.profile_version where profile_id = ? and version = ?",
                Integer.class, profile.profileId(), profile.version());
        if (snapshot != null && snapshot > 0) return;
        try {
            jdbc.update("insert into model.profile_version(profile_id, version, configuration_hash, snapshot, status) values (?, ?, ?, ?::jsonb, 'ACTIVE') on conflict do nothing",
                    profile.profileId(), profile.version(), profile.configurationHash(), json.writeValueAsString(profile));
        } catch (com.fasterxml.jackson.core.JsonProcessingException invalid) {
            throw new IllegalStateException("Unable to persist model profile definition.", invalid);
        }
    }

    private ModelProfileSnapshot read(String snapshot) {
        try { return json.readValue(snapshot, ModelProfileSnapshot.class); }
        catch (Exception invalid) { throw new IllegalStateException("Stored model profile snapshot is invalid.", invalid); }
    }

    private record ProfileRow(String hash, ModelProfileSnapshot snapshot, String status) { }
}
