package io.eaf.bootstrap;

import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/** 只在显式容量检查启用的合成模型延迟，不触发 Provider 网络或真实费用。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "p20.worker.slow-model", havingValue = "true")
public class P20SlowModelConfiguration {
    @Bean
    @Primary
    ModelGateway p20SlowModelGateway(@Qualifier("modelGateway") ModelGateway deterministic,
                                     @Value("${p20.worker.model-delay:PT0.25S}") Duration delay) {
        return new ModelGateway() {
            @Override
            public ModelResult call(ModelRequest request) {
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("合成模型延迟被中断。", interrupted);
                }
                return deterministic.call(request);
            }

            @Override public int callCount() { return deterministic.callCount(); }
            @Override public io.eaf.model.api.ModelBillingProfile billingProfile() { return deterministic.billingProfile(); }
            @Override public String outboundDataScope() { return deterministic.outboundDataScope(); }
        };
    }
}
