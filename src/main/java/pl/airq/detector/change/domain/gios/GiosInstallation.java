package pl.airq.detector.change.domain.gios;

import io.quarkus.runtime.annotations.RegisterForReflection;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.process.event.AppEvent;

import static pl.airq.detector.change.process.TopicConstants.GIOS_INSTALLATION_TOPIC;

@RegisterForReflection
public class GiosInstallation extends AppEvent<GiosInstallationPayload> {

    GiosInstallation(GiosInstallationPayload payload) {
        super(payload);
    }

    @Override
    public String defaultTopic() {
        return GIOS_INSTALLATION_TOPIC;
    }

    public static GiosInstallation from(GiosInstallationEventType type, Installation installation) {
        return new GiosInstallation(new GiosInstallationPayload(type, installation));
    }
}
