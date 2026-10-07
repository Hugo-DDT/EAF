package io.eaf.evaluation.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;

/** 可直接运行的轻量评分合同自检：java -ea ...ScenarioScorerCheck。 */
public final class ScenarioScorerCheck {
    private ScenarioScorerCheck() { }

    public static void main(String[] args) {
        var scorer = new ScenarioScorer(new ObjectMapper());
        var json = new ObjectMapper();
        assert "bd5cd327a1f43de36120843e933c3f41cbe301706c558e60b52ad6fbfdeede9c".equals(
                ScenarioDatasetHash.answer(json, "IT", "READY", "[]"));
        var entries = java.util.List.of(
                new ScenarioDatasetHash.Entry("dev-facility-light", "DEV", "b11384693e1cc2f2d9d9783c723140091ed2ee9fa306b49d796887a6036ff761", "33ddf39defd125ce044b6956f1a5f18475bfc757143bfc1dbab4dbdd3c391c3b"),
                new ScenarioDatasetHash.Entry("dev-it-access", "DEV", "a6084134ddaf0c3c4f60865fa2cd54b1717792264a82aed6f25562a881115f6a", "bd5cd327a1f43de36120843e933c3f41cbe301706c558e60b52ad6fbfdeede9c"),
                new ScenarioDatasetHash.Entry("dev-needs-owner", "DEV", "c2ef0cbf488eb8f16d874128c81c30bc41f495d435738b4823ef75e95d0a7823", "8838cd657686d6c89b681c24c3c87fbe13141ea86aad6fb866acf4fc586a1637"),
                new ScenarioDatasetHash.Entry("dev-no-evidence", "DEV", "4f7a970369501d5a867c6327a1ffe5b6436d8f75f4a63ba31979b085b51d35a4", "38b577160b9f66f63fe6f7b1ec0c91cf67d6551d0d10683793642c7263003090"),
                new ScenarioDatasetHash.Entry("heldout-hr-leave", "HELD_OUT", "27a317838f19b5f5e5508aa88f879cadf36bd82131300a0a8156b1122d03a0d7", "b7ac37d23259e0e0157db98c3e10b7079eb85c86876b6af6095bf67a2592ee5c"));
        assert "9c5e03dd7860ca97d2d6f54ae74689421e156f6737c77db4eea7cf1e36d98d62".equals(
                ScenarioDatasetHash.dataset("service-request-synthetic", "1.0.0", "service-request-rubric-v1", entries));
        String ready = "{\"outcome\":\"READY\",\"category\":\"IT\",\"title\":\"账号开通\","
                + "\"summary\":\"提供账号\",\"handlingSuggestion\":\"联系服务台\",\"citations\":[\"doc-1\"],"
                + "\"questions\":[],\"contextRefs\":[],\"evaluationOnly\":true,\"submittable\":false,\"readyToSubmit\":false}";
        var correct = scorer.score("SUCCEEDED", ready, "IT", "READY", java.util.List.of("doc-1"),
                java.util.List.of(), 0, 0);
        assert "PASS".equals(correct.structure().status());
        assert "PASS".equals(correct.category().status());
        assert "PASS".equals(correct.outcome().status());
        assert "PASS".equals(correct.citationBinding().status());
        assert "PASS".equals(correct.readOnlySafety().status());

        var unbound = scorer.score("SUCCEEDED", ready, "IT", "READY", java.util.List.of("other-doc"),
                java.util.List.of(), 0, 0);
        assert "FAIL".equals(unbound.citationBinding().status());

        var failed = scorer.score("ERROR", null, "IT", "READY", java.util.List.of(),
                java.util.List.of("doc-1:1:build:chunk"), 0, 0);
        assert "FAIL".equals(failed.category().status());
        assert failed.category().denominator() == 1 && failed.category().scored() == 0;
        assert "FAIL".equals(failed.evidenceHit().status());
        assert failed.evidenceHit().denominator() == 1 && failed.evidenceHit().scored() == 0;

        var wrote = scorer.score("SUCCEEDED", ready, "IT", "READY", java.util.List.of("doc-1"),
                java.util.List.of(), 1, 0);
        assert "FAIL".equals(wrote.readOnlySafety().status());
    }
}
