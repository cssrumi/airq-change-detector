package pl.airq.detector.change.util;

import java.lang.reflect.Constructor;
import java.time.OffsetDateTime;
import pl.airq.common.domain.gios.Installation;

import static org.junit.jupiter.api.Assertions.fail;

public class InstallationFactory {

    public static Installation installationWithPm10(Long id, Float pm10) {
        return createInstanceWith(id, pm10, Code.PM10, OffsetDateTime.now());
    }

    public static Installation installationWithPm10(Long id, Float pm10, OffsetDateTime timestamp) {
        return createInstanceWith(id, pm10, Code.PM10, timestamp);
    }

    public static Installation installationWithPm25(Long id, Float pm25) {
        return createInstanceWith(id, pm25, Code.PM25, OffsetDateTime.now());
    }

    public static Installation installationWithPm25(Long id, Float pm25, OffsetDateTime timestamp) {
        return createInstanceWith(id, pm25, Code.PM25, timestamp);
    }

    private static Installation createInstanceWith(Long id, Float value, Code code, OffsetDateTime timestamp) {
        try {
            final Constructor<?> constructor = Installation.class.getDeclaredConstructors()[0];
            constructor.setAccessible(true);
            return (Installation) constructor.newInstance(
                    id,
                    "Installation" + id,
                    timestamp,
                    value,
                    Float.valueOf(1f),
                    Float.valueOf(1f),
                    code.name()
            );
        } catch (Exception e) {
            fail("Unable to create Installation instance", e);
            return null;
        }
    }

}
