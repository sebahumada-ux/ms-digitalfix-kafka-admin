package cl.duoc.digitalfix.kafka.admin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class AdminClientConfig {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(
            @Value("${spring.kafka.bootstrap-servers}") String brokers) {

        return AdminClient.create(
                Map.of(
                        org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        brokers
                )
        );
    }
}