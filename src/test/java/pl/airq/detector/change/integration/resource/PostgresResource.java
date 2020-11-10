package pl.airq.detector.change.integration.resource;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresResource.class);

    private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:11");

    @Override
    public Map<String, String> start() {
        container.start();
        final Map<String, String> config = Map.of(
                "quarkus.datasource.username", container.getUsername(),
                "quarkus.datasource.password", container.getPassword(),
                "quarkus.datasource.jdbc.url", container.getJdbcUrl(),
                "quarkus.datasource.reactive.url", container.getJdbcUrl().replace("jdbc:", "")
        );

        LOGGER.info("Postgres config: {}", config);
        return config;
    }

    @Override
    public void stop() {
        container.stop();
    }
}
