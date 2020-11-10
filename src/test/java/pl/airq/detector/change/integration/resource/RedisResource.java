package pl.airq.detector.change.integration.resource;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class RedisResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisResource.class);

    private final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:5-alpine")).withExposedPorts(6379);

    @Override
    public Map<String, String> start() {
        redis.start();
        Map<String, String> config = Map.of("quarkus.redis.hosts", "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());

        LOGGER.info("RedisResource config: {}", config);
        return config;
    }

    @Override
    public void stop() {
        redis.stop();
    }
}
