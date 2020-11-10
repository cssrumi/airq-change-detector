package pl.airq.detector.change.integration;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.domain.gios.InstallationQuery;
import pl.airq.common.kafka.AirqEventDeserializer;
import pl.airq.common.kafka.TSKeyDeserializer;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationCreatedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationDeletedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationEventPayload;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationUpdatedEvent;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.common.store.Store;
import pl.airq.common.store.key.TSFKey;
import pl.airq.common.store.key.TSKey;
import pl.airq.detector.change.domain.ChangeDetectorPublicProxy;
import pl.airq.detector.change.integration.resource.KafkaResource;
import pl.airq.detector.change.integration.resource.RedisResource;
import pl.airq.detector.change.util.InstallationFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@QuarkusTestResource(KafkaResource.class)
@QuarkusTestResource(RedisResource.class)
@QuarkusTest
class IntegrationTest {

    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(5);
    private final Map<TSKey, AirqEvent<GiosInstallationEventPayload>> eventsMap = new ConcurrentHashMap<>();
    private final List<AirqEvent<GiosInstallationEventPayload>> eventsList = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean shouldConsume = new AtomicBoolean(true);

    @InjectMock
    InstallationQuery query;

    @Inject
    KafkaConsumer<TSKey, AirqEvent<GiosInstallationEventPayload>> consumer;
    @Inject
    ChangeDetectorPublicProxy changeDetector;
    @Inject
    Store<TSFKey, Installation> store;

    @ConfigProperty(name = "mp.messaging.outgoing.gios-installation.topic")
    String topic;

    @BeforeAll
    void startConsuming() {
        executor.submit(() -> {
            while (shouldConsume.get()) {
                consumer.poll(Duration.ofMillis(100))
                        .records(topic)
                        .forEach(record -> {
                            eventsMap.put(record.key(), record.value());
                            eventsList.add(record.value());
                        });
            }
        });
    }

    @AfterAll
    void stopConsuming() {
        shouldConsume.set(false);
        executor.shutdown();
    }

    @BeforeEach
    void beforeEach() {
        eventsMap.clear();
        eventsList.clear();
        store.getMap()
             .map(Map::keySet)
             .onItem().ifNull().continueWith(Collections::emptySet)
             .onItem().transformToMulti(Multi.createFrom()::iterable)
             .onItem().call(key -> store.delete(key))
             .collectItems().asList()
             .await().atMost(DEFAULT_DURATION);

        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().nullItem());
    }

    @Test
    void whenStoreIsEmpty_andNewInstallationInQuery_expectGiosInstallationCreated() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation installation = InstallationFactory.installationWithPm10(1L, 20f, timestamp);

        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().item(Set.of(installation)));

        changeDetector.findChanges();
        AirqEvent<GiosInstallationEventPayload> event = awaitForEvent(TSKey.from(installation));

        assertThat(event).isInstanceOf(GiosInstallationCreatedEvent.class);
        assertThat(event.payload).isNotNull();
        assertThat(event.payload.installation).isEqualTo(installation);
        assertThat(eventsList).hasSize(1);
        assertThat(store.getAll().await().atMost(DEFAULT_DURATION)).hasSize(1);
    }

    @Test
    void whenStoreIsNotEmpty_andUpdatedInstallationInQuery_expectGiosInstallationUpdated() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation installation = InstallationFactory.installationWithPm10(1L, 20f, timestamp);
        Installation updatedInstallation = InstallationFactory.installationWithPm10(1L, 21f, timestamp);

        store.upsert(TSFKey.from(installation), installation)
             .await().atMost(DEFAULT_DURATION);
        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().item(Set.of(updatedInstallation)));

        changeDetector.findChanges();
        AirqEvent<GiosInstallationEventPayload> event = awaitForEvent(TSKey.from(installation));

        assertThat(event).isInstanceOf(GiosInstallationUpdatedEvent.class);
        assertThat(event.payload).isNotNull();
        assertThat(event.payload.installation).isEqualTo(updatedInstallation);
        assertThat(eventsList).hasSize(1);
        assertThat(store.getAll().await().atMost(DEFAULT_DURATION)).hasSize(1);
    }

    @Test
    void whenStoreIsNotEmpty_andInstallationNotInQuery_expectGiosInstallationDeleted() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation installation = InstallationFactory.installationWithPm10(1L, 20f, timestamp);

        store.upsert(TSFKey.from(installation), installation)
             .await().atMost(DEFAULT_DURATION);

        changeDetector.findChanges();
        AirqEvent<GiosInstallationEventPayload> event = awaitForEvent(TSKey.from(installation));

        assertThat(event).isInstanceOf(GiosInstallationDeletedEvent.class);
        assertThat(event.payload).isNotNull();
        assertThat(event.payload.installation).isEqualTo(installation);
        assertThat(eventsList).hasSize(1);
        assertThat(store.getAll().await().atMost(DEFAULT_DURATION)).hasSize(0);
    }

    private AirqEvent<GiosInstallationEventPayload> awaitForEvent(TSKey key) {
        await().atMost(Duration.ofSeconds(5)).until(() -> eventsMap.containsKey(key));
        return eventsMap.get(key);
    }

    @Dependent
    static class KafkaTestConfiguration {

        @ConfigProperty(name = "kafka.bootstrap.servers")
        String bootstrapServers;
        @ConfigProperty(name = "mp.messaging.outgoing.gios-installation.topic")
        String topic;

        @Inject
        EventParser parser;

        @Produces
        KafkaConsumer<TSKey, AirqEvent<GiosInstallationEventPayload>> kafkaConsumer() {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);
            properties.put("enable.auto.commit", "true");
            properties.put("group.id", "airq-change-detector-int-test");
            properties.put("auto.offset.reset", "earliest");

            KafkaConsumer<TSKey, AirqEvent<GiosInstallationEventPayload>> consumer = new KafkaConsumer<>(
                    properties,
                    new TSKeyDeserializer(),
                    new AirqEventDeserializer<>(parser)
            );
            consumer.subscribe(Collections.singleton(topic));
            return consumer;
        }

    }
}
