package pl.airq.detector.change.domain;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.time.OffsetDateTime;
import java.util.function.BiFunction;
import pl.airq.common.domain.gios.GiosMeasurementCreatedEvent;
import pl.airq.common.domain.gios.GiosMeasurementDeletedEvent;
import pl.airq.common.domain.gios.GiosMeasurementEventPayload;
import pl.airq.common.domain.gios.GiosMeasurementUpdatedEvent;
import pl.airq.common.process.event.AirqEvent;

@RegisterForReflection
public enum GiosMEventType {
    CREATED(GiosMeasurementCreatedEvent::new),
    UPDATED(GiosMeasurementUpdatedEvent::new),
    DELETED(GiosMeasurementDeletedEvent::new);

    private final BiFunction<OffsetDateTime, GiosMeasurementEventPayload, AirqEvent<GiosMeasurementEventPayload>> parseFunction;

    GiosMEventType(BiFunction<OffsetDateTime, GiosMeasurementEventPayload, AirqEvent<GiosMeasurementEventPayload>> parseFunction) {
        this.parseFunction = parseFunction;
    }

    public AirqEvent<GiosMeasurementEventPayload> intoAirqEvent(GiosMeasurement appEvent) {
        return appEvent.payload.type.parseFunction.apply(appEvent.timestamp, new GiosMeasurementEventPayload(appEvent.payload.installation));
    }
}
