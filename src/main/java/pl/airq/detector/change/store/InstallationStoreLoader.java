package pl.airq.detector.change.store;

import com.google.common.base.Preconditions;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.gios.installation.Installation;
import pl.airq.common.domain.gios.installation.InstallationQuery;
import pl.airq.common.store.Store;
import pl.airq.common.store.key.TSFKey;

@ApplicationScoped
class InstallationStoreLoader {

    private static final String TIME_UNIT_VALIDATION_FAILED = "TimeUnit accept only HOURS and DAYS";
    private static final String SINCE_LAST_VALIDATION_FAILED = "SinceLast accept only value bigger than 0";
    private static final String SINCE_LAST_CONFIG = "change-detector.bootstrap.sinceLast";
    private static final String TIME_UNIT_CONFIG = "change-detector.bootstrap.timeUnit";
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallationStoreLoader.class);

    private final InstallationQuery query;
    private final Store<TSFKey, Installation> store;
    private final Long loadSinceLast;
    private final Event<InstallationStoreReady> storeReadyEvent;

    @Inject
    InstallationStoreLoader(InstallationQuery query,
                            Store<TSFKey, Installation> store,
                            Event<InstallationStoreReady> storeReadyEvent,
                            @ConfigProperty(name = SINCE_LAST_CONFIG) Integer loadSinceLast,
                            @ConfigProperty(name = TIME_UNIT_CONFIG) ChronoUnit timeUnit) {
        Preconditions.checkArgument(timeUnit == ChronoUnit.DAYS || timeUnit == ChronoUnit.HOURS, TIME_UNIT_VALIDATION_FAILED);
        Preconditions.checkArgument(loadSinceLast > 0, SINCE_LAST_VALIDATION_FAILED);
        this.query = query;
        this.store = store;
        this.storeReadyEvent = storeReadyEvent;
        this.loadSinceLast = Duration.of(loadSinceLast, timeUnit).toHours();
    }

    void bootstrapStore(@Observes StartupEvent startup) {
        LOGGER.info("Bootstrapping Installation Store...");
        int loadedCount = query.getAllWithPMSinceLastNHours(loadSinceLast.intValue())
                               .toMulti().flatMap(installations -> Multi.createFrom().iterable(installations))
                               .invokeUni(installation -> upsertIfNotExists(installation, TSFKey.from(installation)))
                               .collectItems().asList()
                               .await().atMost(Duration.ofSeconds(60))
                               .size();
        LOGGER.info("{} objects loaded into the store", loadedCount);
        storeReadyEvent.fireAsync(new InstallationStoreReady());
    }

    private Uni<Installation> upsertIfNotExists(Installation installation, TSFKey key) {
        return store.pull(key).onItem().ifNull().switchTo(store.upsert(key, installation));
    }
}
