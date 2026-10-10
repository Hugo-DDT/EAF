package io.eaf.workflow.infrastructure;

import io.eaf.workflow.api.WorkflowAutomationService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class P30AutomationDispatcher {
    private static final Logger LOG = Logger.getLogger(P30AutomationDispatcher.class.getName());
    private final WorkflowAutomationService automations;
    private final boolean enabled;

    public P30AutomationDispatcher(WorkflowAutomationService automations,
            @Value("${eaf.workflow.automation.enabled:true}") boolean enabled) {
        this.automations = automations;
        this.enabled = enabled;
    }

    @Scheduled(fixedDelayString = "${eaf.workflow.automation.poll-delay:1000}")
    public void dispatch() {
        if (!enabled) return;
        try {
            automations.consumeOneEvent();
            for (int i = 0; i < 8 && automations.dispatchOne(); i++) { }
        } catch (RuntimeException failure) {
            LOG.log(Level.WARNING, "P30 automation scan failed; the next bounded poll will retry.", failure);
        }
    }
}
