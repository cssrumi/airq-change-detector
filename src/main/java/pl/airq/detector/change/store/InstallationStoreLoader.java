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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.domain.gios.InstallationQuery;
import pl.airq.common.store.Store;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.config.ChangeDetectorProperties;

@ApplicationScoped
class InstallationStoreLoader {

    private static final String TIME_UNIT_VALIDATION_FAILED = "TimeUnit accept only HOURS and DAYS";
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
                            ChangeDetectorProperties properties) {
        Preconditions.checkArgument(
                properties.getDetect().getTimeUnit() == ChronoUnit.DAYS || properties.getDetect().getTimeUnit() == ChronoUnit.HOURS,
                TIME_UNIT_VALIDATION_FAILED
        );
        this.query = query;
        this.store = store;
        this.storeReadyEvent = storeReadyEvent;
        this.loadSinceLast = properties.getDetect().sinceLastDuration().toHours();
        this.pullFromDb = properties.getLoader().getPullFromDb();
        this.maxAwait = Duration.ofSeconds(properties.getLoader().getMaxAwaitInSeconds());
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
