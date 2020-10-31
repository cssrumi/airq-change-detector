package pl.airq.detector.change.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.client.reactive.ReactiveRedisClient;
import java.time.Duration;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.store.Store;
import pl.airq.common.store.StoreBuilder;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.store.InstallationRedisLayer;

@Dependent
public class StoreProvider {

    @Singleton
    @Produces
    Store<TSFKey, Installation> installationStore(ReactiveRedisClient redis, AppEventBus bus, ObjectMapper mapper,
                                                  ChangeDetectorProperties properties) {
        final Duration storeExpire = properties.getStore().getExpire().duration();
        final InstallationRedisLayer redisLayer = new InstallationRedisLayer(redis, bus, mapper, storeExpire);
        return new StoreBuilder<TSFKey, Installation>().withGuavaCacheOnTop(storeExpire)
                                                       .withLayer(redisLayer)
                                                       .build();
    }
}
