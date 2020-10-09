package pl.airq.detector.change.process;

import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import pl.airq.common.domain.gios.GiosMeasurementEventPayload;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.MutinyUtils;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.detector.change.domain.GiosMeasurement;

import static pl.airq.detector.change.process.TopicConstants.GIOS_MEASUREMENT_TOPIC;

@ApplicationScoped
class GiosMeasurementEventConsumer {

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
        return Uni.createFrom().item(event)
                  .invoke(e -> {
                      final AirqEvent<GiosMeasurementEventPayload> airqEvent = e.payload.type.intoAirqEvent(e);
                      final String rawEvent = parser.parse(airqEvent);
                      switch (e.payload.type) {
                          case CREATED:
                              createdEmitter.send(rawEvent);
                              break;
                          case UPDATED:
                              updatedEmitter.send(rawEvent);
                              break;
                          case DELETED:
                              deletedEmitter.send(rawEvent);
                              break;
                      }
                  })
                  .flatMap(MutinyUtils::ignoreUniResult);
    }
}
