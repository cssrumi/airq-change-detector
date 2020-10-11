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
    private static final String SINCE_LAST_CONFIG = "change-detector.detect.sinceLast";
    private static final String TIME_UNIT_CONFIG = "change-detector.detect.timeUnit";
    private static final String PULL_FROM_DB_CONFIG = "change-detector.loader.pullFromDb";
    private static final String MAX_AWAIT_IN_SECONDS_CONFIG = "change-detector.loader.maxAwaitInSeconds";
    private static final Logger LOGGER = LoggerFactory.getLogger(InstallationStoreLoader.class);

    private final InstallationQuery query;
    private final Store<TSFKey, Installation> store;
    private final Event<InstallationStoreReady> storeReadyEvent;
    private final Long loadSinceLast;
    private final Boolean pullFromDb;
    private final Duration maxAwait;

    @Inject
    InstallationStoreLoader(InstallationQuery query,
                            Store<TSFKey, Installation> store,
                            Event<InstallationStoreReady> storeReadyEvent,
                            @ConfigProperty(name = SINCE_LAST_CONFIG) Integer loadSinceLast,
                            @ConfigProperty(name = TIME_UNIT_CONFIG) ChronoUnit timeUnit,
                            @ConfigProperty(name = PULL_FROM_DB_CONFIG) Boolean pullFromDb,
                            @ConfigProperty(name = MAX_AWAIT_IN_SECONDS_CONFIG, defaultValue = "60") Long maxAwaitInSeconds) {
        Preconditions.checkArgument(timeUnit == ChronoUnit.DAYS || timeUnit == ChronoUnit.HOURS, TIME_UNIT_VALIDATION_FAILED);
        Preconditions.checkArgument(loadSinceLast > 0, SINCE_LAST_VALIDATION_FAILED);
        this.query = query;
        this.store = store;
        this.storeReadyEvent = storeReadyEvent;
        this.loadSinceLast = Duration.of(loadSinceLast, timeUnit).toHours();
        this.pullFromDb = pullFromDb;
        this.maxAwait = Duration.ofSeconds(maxAwaitInSeconds);
    }

    void bootstrapStore(@Observes StartupEvent startup) {
        LOGGER.info("Bootstrapping Installation Store...");
        int loadedCount = pullFromDb ? bootstrapFromDb() : bootstrapFromStore();
        LOGGER.info("{} objects loaded into the store", loadedCount);
        storeReadyEvent.fireAsync(new InstallationStoreReady());
    }

    private int bootstrapFromStore() {
        return store.pullAll(value -> true, TSFKey::from)
                    .await().atMost(maxAwait)
                    .size();
    }

    private int bootstrapFromDb() {
        return query.getAllWithPMSinceLastNHours(loadSinceLast.intValue())
                    .toMulti().flatMap(Multi.createFrom()::iterable)
                    .invokeUni(installation -> upsertIfNotExists(installation, TSFKey.from(installation)))
                    .collectItems().asList()
                    .await().atMost(maxAwait)
                    .size();
    }

    private Uni<Installation> upsertIfNotExists(Installation installation, TSFKey key) {
        return store.pull(key).onItem().ifNull().switchTo(store.upsert(key, installation));
    }
}
