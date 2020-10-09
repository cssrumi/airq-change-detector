package pl.airq.detector.change.domain;

import io.quarkus.runtime.annotations.RegisterForReflection;
import pl.airq.common.process.event.AppEvent;

import static pl.airq.detector.change.process.TopicConstants.GIOS_MEASUREMENT_TOPIC;

@RegisterForReflection
public class GiosMeasurement extends AppEvent<GiosMeasurementPayload> {

    GiosMeasurement(GiosMeasurementPayload payload) {
        super(payload);
    }

    @Override
    public String defaultTopic() {
        return GIOS_MEASUREMENT_TOPIC;
    }
}
