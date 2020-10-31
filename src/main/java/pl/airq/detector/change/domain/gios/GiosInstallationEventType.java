package pl.airq.detector.change.domain.gios;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.time.OffsetDateTime;
import java.util.function.BiFunction;

import pl.airq.common.process.ctx.gios.installation.GiosInstallationCreatedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationDeletedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationEventPayload;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationUpdatedEvent;
import pl.airq.common.process.event.AirqEvent;

@RegisterForReflection
public enum GiosInstallationEventType {
    CREATED(GiosInstallationCreatedEvent::new),
    UPDATED(GiosInstallationUpdatedEvent::new),
    DELETED(GiosInstallationDeletedEvent::new);

    private final BiFunction<OffsetDateTime, GiosInstallationEventPayload, AirqEvent<GiosInstallationEventPayload>> parseFunction;

    GiosInstallationEventType(BiFunction<OffsetDateTime, GiosInstallationEventPayload, AirqEvent<GiosInstallationEventPayload>> parseFunction) {
        this.parseFunction = parseFunction;
    }

    public AirqEvent<GiosInstallationEventPayload> intoAirqEvent(GiosInstallation appEvent) {
        return appEvent.payload.type.parseFunction.apply(appEvent.timestamp, new GiosInstallationEventPayload(appEvent.payload.installation));
    }
}
