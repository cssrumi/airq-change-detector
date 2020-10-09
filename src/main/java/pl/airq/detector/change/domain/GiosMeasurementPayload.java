package pl.airq.detector.change.domain;

import io.quarkus.runtime.annotations.RegisterForReflection;
import pl.airq.common.domain.gios.installation.Installation;
import pl.airq.common.process.Payload;

@RegisterForReflection
public class GiosMeasurementPayload implements Payload {

    public final GiosMEventType type;
    public final Installation installation;

    public GiosMeasurementPayload(GiosMEventType type, Installation installation) {
        this.type = type;
        this.installation = installation;
    }

    @Override
    public String toString() {
        return "GiosMeasurementPayload{" +
                "type=" + type +
                ", installation=" + installation +
                '}';
    }
}
