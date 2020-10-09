package pl.airq.detector.change.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.client.reactive.ReactiveRedisClient;
import java.time.temporal.ChronoUnit;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import pl.airq.common.domain.gios.installation.Installation;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.store.Store;
import pl.airq.common.store.StoreBuilder;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.store.InstallationRedisLayer;

@Dependent
public class StoreProvider {

    private static final String EXPIRE_IN_CONFIG = "change-detector.redis.expire.in";
    private static final String EXPIRE_TIME_UNIT_CONFIG = "change-detector.redis.expire.timeUnit";

    @Singleton
    @Produces
    Store<TSFKey, Installation> installationStore(ReactiveRedisClient redis, AppEventBus bus, ObjectMapper mapper,
                                                  @ConfigProperty(name = EXPIRE_IN_CONFIG) Long expireIn,
                                                  @ConfigProperty(name = EXPIRE_TIME_UNIT_CONFIG) ChronoUnit expireTimeUnit) {
        final InstallationRedisLayer redisLayer = new InstallationRedisLayer(redis, bus, mapper, expireIn, expireTimeUnit);
        return new StoreBuilder<TSFKey, Installation>().withInMemoryAtTop()
                                                       .withLayer(redisLayer)
                                                       .build();
    }
}
