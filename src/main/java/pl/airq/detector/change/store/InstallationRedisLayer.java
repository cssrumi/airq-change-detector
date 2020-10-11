package pl.airq.detector.change.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.quarkus.redis.client.reactive.ReactiveRedisClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Response;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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

    protected Uni<Set<Installation>> getAllFrom(List<String> rawKeys) {
        return Iterables.isEmpty(rawKeys) ? Uni.createFrom().item(Set.of()) : reactiveRedisClient.mget(rawKeys)
                                                                                            .map(Response::toString)
                                                                                            .map(this::deserializeSet);
    }

    @Override
    public Uni<Set<Installation>> getAll() {
        return reactiveRedisClient.keys("*")
                                  .onItem().transformToMulti(Multi.createFrom()::iterable)
                                  .map(Response::toString)
                                  .collectItems().asList()
                                  .flatMap(this::getAllFrom);
    }

    @Override
    public Uni<Set<Installation>> getAll(Predicate<Installation> predicate) {
        return getAll().onItem().transformToMulti(Multi.createFrom()::iterable)
                       .filter(predicate)
                       .collectItems().with(Collectors.toUnmodifiableSet());
    }

    protected Uni<Set<Installation>> getAllWithKeyPredicate(Predicate<TSFKey> keyPredicate) {
        return reactiveRedisClient.keys("*")
                                  .onItem().transformToMulti(Multi.createFrom()::iterable)
                                  .map(Response::toString)
                                  .filter(key -> keyPredicate.test(TSFKey.from(key)))
                                  .collectItems().asList()
                                  .flatMap(this::getAllFrom);
    }


    protected Uni<Map<TSFKey, Installation>> getMapFrom(Uni<Set<Installation>> uniSet) {
        return uniSet.onItem().transformToMulti(Multi.createFrom()::iterable)
                     .collectItems().asMap(TSFKey::from, installation -> installation);
    }

    @Override
    public Uni<Map<TSFKey, Installation>> getMap() {
        return getMapFrom(getAll());
    }

    @Override
    public Uni<Map<TSFKey, Installation>> getMap(Predicate<TSFKey> keyPredicate) {
        return getMapFrom(getAllWithKeyPredicate(keyPredicate));
    }

    @Override
    public Uni<Installation> upsert(TSFKey key, Installation value) {
        return reactiveRedisClient.setex(key.value(), expireInSeconds, serializeValue(value))
                                  .invoke(ignore -> LOGGER.debug("Set key: {}, value: {}", key.value(), value))
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

    private Set<Installation> deserializeSet(String rawSet) {
        try {
            return Sets.newHashSet(mapper.readValue(rawSet, Installation[].class));
        } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to deserialize set {}", rawSet);
            throw new DeserializationException(String.format("Unable to deserialize set %s", rawSet));
        }
    }
}
