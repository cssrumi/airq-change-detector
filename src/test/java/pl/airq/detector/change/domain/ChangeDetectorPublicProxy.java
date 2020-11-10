package pl.airq.detector.change.domain;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import pl.airq.detector.change.store.InstallationStoreReady;

@ApplicationScoped
public class ChangeDetectorPublicProxy {

    private static final String FIND_CHANGES = "findChanges";

    @Inject
    ChangeDetector changeDetector;

    public void installationStoreReady() {
        changeDetector.installationStoreReady(new InstallationStoreReady());
    }

    public void cronInvoker() {
        changeDetector.cronInvoker();
    }

    public void findChanges() {
        changeDetector.findChanges();
    }
}
