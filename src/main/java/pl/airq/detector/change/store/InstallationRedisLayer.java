package pl.airq.detector.change.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.client.reactive.ReactiveRedisClient;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Response;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.exception.DeserializationException;
import pl.airq.common.domain.exception.SerializationException;
import pl.airq.common.domain.gios.installation.Installation;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.process.MutinyUtils;
import pl.airq.common.process.failure.FailureFactory;
import pl.airq.common.store.key.TSFKey;
import pl.airq.common.store.layer.StoreLayer;

public class InstallationRedisLayer implements StoreLayer<TSFKey, Installation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallationRedisLayer.class);
    private final ReactiveRedisClient reactiveRedisClient;
    private final AppEventBus bus;
    private final ObjectMapper mapper;
    private final String expireInSeconds;

    public InstallationRedisLayer(ReactiveRedisClient reactiveRedisClient, AppEventBus bus, ObjectMapper mapper,
                                  Long expireIn, ChronoUnit expireTimeUnit) {
        this.reactiveRedisClient = reactiveRedisClient;
        this.bus = bus;
        this.mapper = mapper;
        this.expireInSeconds = Long.toString(Duration.of(expireIn, expireTimeUnit).toSeconds());
    }

    @PostConstruct
    void logConfig() {
        LOGGER.info("{} initialized. Cache expire set at: {} seconds", InstallationRedisLayer.class.getSimpleName(), expireInSeconds);
    }

    @Override
    public Uni<Installation> get(TSFKey key) {
        return reactiveRedisClient.get(key.value())
                                  .onItem().ifNotNull().transform(Response::toString)
                                  .onItem().ifNotNull().transform(this::deserializeValue);
    }

    @Override
    public Uni<Installation> pull(TSFKey key) {
        return get(key);
    }

    @Override
    public Uni<Installation> upsert(TSFKey key, Installation value) {
        return reactiveRedisClient.setex(key.value(), expireInSeconds, serializeValue(value))
                                  .invoke(ignore -> LOGGER.info("Set key: {}, value: {}", key.value(), value))
                                  .onFailure().invoke(throwable -> bus.publish(FailureFactory.fromThrowable(throwable)))
                                  .map(ignore -> value);
    }

    @Override
    public Uni<Void> delete(TSFKey key) {
        return reactiveRedisClient.del(List.of(key.value()))
                                  .onItem().transformToUni(MutinyUtils::ignoreUniResult);
    }

    private String serializeValue(Installation value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to serialize {}", value);
            throw new SerializationException(String.format("Unable to serialize %s", value), e);
        }
    }

    private Installation deserializeValue(String rawValue) {
        try {
            return mapper.readValue(rawValue, Installation.class);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to deserialize {}", rawValue);
            throw new DeserializationException(String.format("Unable to deserialize %s", rawValue));
        }
    }
}
