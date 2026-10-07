package io.eaf.shared;

import java.util.UUID;

public final class Ids {
    private Ids() { }
    public static final UUID TENANT_A = UUID.fromString("70000000-0000-4000-8000-000000000001");
    public static final UUID TENANT_B = UUID.fromString("70000000-0000-4000-8000-000000000002");
    public static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    public static final UUID BOB = UUID.fromString("80000000-0000-4000-8000-000000000002");
    public static final UUID CAROL = UUID.fromString("80000000-0000-4000-8000-000000000003");
    public static final UUID WORKSPACE_A = UUID.fromString("10000000-0000-4000-8000-000000000001");
    public static final UUID WORKSPACE_A2 = UUID.fromString("10000000-0000-4000-8000-000000000002");
    public static final UUID WORKSPACE_B = UUID.fromString("10000000-0000-4000-8000-000000000003");
    public static final UUID WORKSPACE_B2 = UUID.fromString("10000000-0000-4000-8000-000000000004");
    public static final UUID AGENT_RISK = UUID.fromString("20000000-0000-4000-8000-000000000001");
    public static final UUID AGENT_RISK_B = UUID.fromString("20000000-0000-4000-8000-000000000002");
    public static final UUID PROMPT_RISK = UUID.fromString("21000000-0000-4000-8000-000000000001");
    public static final UUID MODEL_DETERMINISTIC = UUID.fromString("22000000-0000-4000-8000-000000000001");
}
// 本文件负责实现 EAF 的 Ids.java 相关代码。
