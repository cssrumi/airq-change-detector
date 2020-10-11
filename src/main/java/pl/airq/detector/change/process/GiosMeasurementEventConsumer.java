package pl.airq.detector.change.process;

import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.gios.GiosMeasurementEventPayload;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.MutinyUtils;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.detector.change.domain.GiosMEventType;
import pl.airq.detector.change.domain.GiosMeasurement;

import static pl.airq.detector.change.process.TopicConstants.GIOS_MEASUREMENT_TOPIC;

@ApplicationScoped
class GiosMeasurementEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GiosMeasurementEventConsumer.class);

    private final AppEventBus bus;
    private final EventParser parser;
    private final Emitter<String> createdEmitter;
    private final Emitter<String> updatedEmitter;
    private final Emitter<String> deletedEmitter;

    @Inject
    GiosMeasurementEventConsumer(AppEventBus bus, EventParser parser,
                                 @Channel("gios-m-created") Emitter<String> createdEmitter,
                                 @Channel("gios-m-updated") Emitter<String> updatedEmitter,
                                 @Channel("gios-m-deleted") Emitter<String> deletedEmitter) {
        this.bus = bus;
        this.parser = parser;
        this.createdEmitter = createdEmitter;
        this.updatedEmitter = updatedEmitter;
        this.deletedEmitter = deletedEmitter;
    }

    @ConsumeEvent(GIOS_MEASUREMENT_TOPIC)
    Uni<Void> consumeEvent(GiosMeasurement event) {
        return Uni.createFrom().voidItem()
                  .invoke(ignore -> {
                      final GiosMEventType type = event.payload.type;
                      final AirqEvent<GiosMeasurementEventPayload> airqEvent = type.intoAirqEvent(event);
                      final String rawEvent = parser.parse(airqEvent);
                      final Message<String> message = Message.of(rawEvent);
                      switch (type) {
                          case CREATED:
                              createdEmitter.send(message);
                              break;
                          case UPDATED:
                              updatedEmitter.send(message);
                              break;
                          case DELETED:
                              deletedEmitter.send(message);
                              break;
                          default:
                              LOGGER.warn("Unsupported event type: {}", type);
                      }
                      LOGGER.info("GiosMeasurementEvent type: {} send.", type);
                  })
                  .flatMap(MutinyUtils::ignoreUniResult);
    }
}
