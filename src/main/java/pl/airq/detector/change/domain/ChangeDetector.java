package pl.airq.detector.change.domain;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.ObservesAsync;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.domain.gios.InstallationQuery;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.process.MutinyUtils;
import pl.airq.common.store.Store;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.config.ChangeDetectorProperties;
import pl.airq.detector.change.domain.gios.GiosInstallation;
import pl.airq.detector.change.domain.gios.GiosInstallationEventType;
import pl.airq.detector.change.domain.gios.GiosInstallationPayload;
import pl.airq.detector.change.store.InstallationStoreReady;

@ApplicationScoped
class ChangeDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDetector.class);

    private final AtomicBoolean isStoreReady = new AtomicBoolean(false);
    private final AppEventBus bus;
    private final Store<TSFKey, Installation> store;
    private final InstallationQuery query;
    private final Long detectSinceLast;
    private final Duration maxProcessAwait;

    @Inject
    public ChangeDetector(AppEventBus bus, Store<TSFKey, Installation> store, InstallationQuery query, ChangeDetectorProperties properties) {
        this.bus = bus;
        this.store = store;
        this.query = query;
        this.detectSinceLast = properties.getDetect().sinceLastDuration().toHours();
        this.maxProcessAwait = properties.getDetect().maxProcessAwaitDuration();
    }

    void installationStoreReady(@ObservesAsync InstallationStoreReady storeReady) {
        isStoreReady.set(true);
        LOGGER.info("{} handled. {} started...", storeReady.getClass().getSimpleName(), ChangeDetector.class.getSimpleName());
        findChanges();
    }

    @Scheduled(cron = "{change-detector.invoke.cron}")
    void cronInvoker() {
        if (isStoreReady.get()) {
            findChanges();
        }
    }

    private void findChanges() {
        final OffsetDateTime oldestValid = oldestValid();
        final Predicate<TSFKey> isNotExpired = key -> key.timestamp().isAfter(oldestValid);
        final Map<TSFKey, Installation> storeState = store.getMap(isNotExpired)
                                                          .await().atMost(Duration.ofSeconds(2));

        long processedCount = query.getAllWithPMSinceLastNHours(detectSinceLast.intValue())
                                   .onItem().transformToMulti(Multi.createFrom()::iterable)
                                   .filter(installation -> installation.timestamp.isAfter(oldestValid))
                                   .invokeUni(installation -> detectEventAndUpsert(installation, storeState))
                                   .collectItems().with(Collectors.counting())
                                   .await().atMost(maxProcessAwait);

        final OffsetDateTime newOldest = oldestValid();
        processedCount += Multi.createFrom().items(storeState.keySet()::stream)
                               .filter(key -> key.timestamp().isAfter(newOldest))
                               .invokeUni(store::delete)
                               .invoke(key -> bus.publish(GiosInstallation.from(GiosInstallationEventType.DELETED, storeState.get(key))))
                               .collectItems().with(Collectors.counting())
                               .await().atMost(maxProcessAwait);

        LOGGER.info("Processed {} objects.", processedCount);
    }

    private OffsetDateTime oldestValid() {
        return OffsetDateTime.now().minusHours(detectSinceLast).plusMinutes(1);
    }

    private Uni<Void> detectEventAndUpsert(Installation installation, Map<TSFKey, Installation> storeState) {
        TSFKey key = TSFKey.from(installation);
        return Uni.createFrom().item(storeState.get(key))
                  .map(fromStore -> detectUpdateOrCreated(installation, fromStore))
                  .invokeUni(shouldUpsert -> shouldUpsert ? store.upsert(key, installation) : Uni.createFrom().voidItem())
                  .invoke(ignore -> storeState.remove(key))
                  .flatMap(MutinyUtils::ignoreUniResult);
    }

    private boolean detectUpdateOrCreated(Installation installation, Installation fromStore) {
        if (fromStore == null && installation != null) {
            bus.publish(GiosInstallation.from(GiosInstallationEventType.CREATED, installation));
            return true;
        }

        if (Objects.equals(installation, fromStore)) {
            return false;
        }

        bus.publish(GiosInstallation.from(GiosInstallationEventType.UPDATED, installation));
        return true;
    }
}
