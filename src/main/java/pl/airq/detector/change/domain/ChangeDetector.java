package pl.airq.detector.change.domain;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.ObservesAsync;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.airq.common.domain.gios.installation.Installation;
import pl.airq.common.domain.gios.installation.InstallationQuery;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.store.Store;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.store.InstallationStoreReady;

@ApplicationScoped
class ChangeDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDetector.class);
    private final AtomicBoolean isStoreReady = new AtomicBoolean(false);
    private final AppEventBus bus;
    private final Store<TSFKey, Installation> store;
    private final InstallationQuery query;

    @Inject
    public ChangeDetector(AppEventBus bus, Store<TSFKey, Installation> store, InstallationQuery query) {
        this.bus = bus;
        this.store = store;
        this.query = query;
    }

    void installationStoreReady(@ObservesAsync InstallationStoreReady storeReady) {
        isStoreReady.set(true);
        LOGGER.info("{} handled. {} started...", storeReady.getClass().getSimpleName(), ChangeDetector.class.getSimpleName());
    }
}
