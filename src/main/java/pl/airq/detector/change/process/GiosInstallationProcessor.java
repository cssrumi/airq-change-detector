package pl.airq.detector.change.process;

import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationEventPayload;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.common.store.key.TSKey;
import pl.airq.detector.change.domain.gios.GiosInstallation;
import pl.airq.detector.change.domain.gios.GiosInstallationEventType;

import static pl.airq.detector.change.process.TopicConstants.GIOS_INSTALLATION_TOPIC;

@ApplicationScoped
class GiosInstallationProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GiosInstallationProcessor.class);

    private final Emitter<String> installationEmitter;
    private final EventParser parser;
    private final String topic;

    @Inject
    GiosInstallationProcessor(@OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 100)
                              @Channel("gios-installation") Emitter<String> installationEmitter,
                              @ConfigProperty(name = "mp.messaging.outgoing.gios-installation.topic") String topic,
                              EventParser parser) {

        this.parser = parser;
        this.installationEmitter = installationEmitter;
        this.topic = topic;
    }

    @ConsumeEvent(GIOS_INSTALLATION_TOPIC)
    Uni<Void> consumeEvent(GiosInstallation event) {
        return Uni.createFrom().voidItem()
                  .invoke(() -> {
                      final GiosInstallationEventType type = event.payload.type;
                      final TSKey key = TSKey.from(event.payload.installation);
                      final AirqEvent<GiosInstallationEventPayload> airqEvent = type.intoAirqEvent(event);
                      final String rawEvent = parser.parse(airqEvent);
                      OutgoingKafkaRecordMetadata<TSKey> metadata = OutgoingKafkaRecordMetadata
                              .<TSKey>builder()
                              .withTopic(topic)
                              .withKey(key)
                              .build();
                      final Message<String> message = Message.of(rawEvent)
                                                             .addMetadata(metadata);
                      installationEmitter.send(message);
                      LOGGER.info("GiosMeasurementEvent type: {} send.", type);
                  });
    }
}
