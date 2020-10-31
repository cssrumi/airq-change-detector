package pl.airq.detector.change.domain.gios;

import io.quarkus.runtime.annotations.RegisterForReflection;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.process.Payload;

@RegisterForReflection
public class GiosInstallationPayload implements Payload {

    public final GiosInstallationEventType type;
    public final Installation installation;

    public GiosInstallationPayload(GiosInstallationEventType type, Installation installation) {
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
