package pl.airq.detector.change.domain;

import io.smallrye.mutiny.Uni;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.domain.gios.InstallationQuery;
import pl.airq.common.process.AppEventBus;
import pl.airq.common.process.event.AppEvent;
import pl.airq.common.store.Store;
import pl.airq.common.store.StoreBuilder;
import pl.airq.common.store.key.TSFKey;
import pl.airq.detector.change.config.ChangeDetectorProperties;
import pl.airq.detector.change.domain.gios.GiosInstallation;
import pl.airq.detector.change.domain.gios.GiosInstallationEventType;
import pl.airq.detector.change.domain.gios.GiosInstallationPayload;
import pl.airq.detector.change.store.InstallationStoreReady;
import pl.airq.detector.change.util.InstallationFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

class ChangeDetectorTest {

    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(5);

    @Mock
    InstallationQuery query;
    @Mock
    AppEventBus bus;
    @Mock
    ChangeDetectorProperties properties;
    @Mock
    ChangeDetectorProperties.Detect detect;

    Store<TSFKey, Installation> store;

    ChangeDetector changeDetector;

    @BeforeEach
    void beforeEach() throws Exception {
        openMocks(this).close();

        when(properties.getDetect()).thenReturn(detect);
        when(detect.maxProcessAwaitDuration()).thenReturn(DEFAULT_DURATION);
        when(detect.sinceLastDuration()).thenReturn(Duration.ofHours(1));
        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().nullItem());

        store = new StoreBuilder<TSFKey, Installation>().withGuavaCacheOnTop(DEFAULT_DURATION).build();
        changeDetector = new ChangeDetector(bus, store, query, properties);
    }

    @Test
    void installationStoreReady_expectIsStoreReadySetToTrueAndNoneEventSent() throws IllegalAccessException {
        Field isStoreReady = FieldUtils.getField(changeDetector.getClass(), "isStoreReady", true);
        boolean before = ((AtomicBoolean) FieldUtils.readField(isStoreReady, changeDetector)).get();

        changeDetector.installationStoreReady(new InstallationStoreReady());

        boolean after = ((AtomicBoolean) FieldUtils.readField(isStoreReady, changeDetector)).get();

        assertThat(before).isFalse();
        assertThat(after).isTrue();
        verifyNoInteractions(bus);
    }

    @Test
    void findChanges_withValueInStoreAndSameValueInQuery_expectNoneEventSent() {
        Installation value = InstallationFactory.installationWithPm10(1L, 10f);

        store.upsert(TSFKey.from(value), value).await().atMost(DEFAULT_DURATION);
        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().item(Set.of(value)));

        changeDetector.findChanges();

        verifyNoInteractions(bus);
    }

    @Test
    void findChanges_withoutValueInStoreButWithValueInQuery_expectGiosInstallationCreatedEvent() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation value = InstallationFactory.installationWithPm10(1L, 10f, timestamp);

        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().item(Set.of(value)));

        ArgumentCaptor<AppEvent<?>> eventCaptor = ArgumentCaptor.forClass(AppEvent.class);
        changeDetector.findChanges();

        verify(bus, times(1)).publish(eventCaptor.capture());
        assertThat(eventCaptor.getAllValues()).hasSize(1);
        assertThat(eventCaptor.getValue()).isInstanceOf(GiosInstallation.class);
        AppEvent<GiosInstallationPayload> capturedEvent = (AppEvent<GiosInstallationPayload>) eventCaptor.getValue();
        assertThat(capturedEvent.payload).isNotNull();
        assertThat(capturedEvent.payload.type).isSameAs(GiosInstallationEventType.CREATED);
        assertThat(capturedEvent.payload.installation).isEqualTo(value);
    }

    @Test
    void findChanges_withValueInStoreAndUpdatedValueInQuery_expectGiosInstallationUpdatedEvent() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation value = InstallationFactory.installationWithPm10(1L, 10f, timestamp);
        Installation updatedValue = InstallationFactory.installationWithPm10(1L, 11f, timestamp);

        store.upsert(TSFKey.from(value), value).await().atMost(DEFAULT_DURATION);
        when(query.getAllWithPMSinceLastNHours(anyInt())).thenReturn(Uni.createFrom().item(Set.of(updatedValue)));

        ArgumentCaptor<AppEvent<?>> eventCaptor = ArgumentCaptor.forClass(AppEvent.class);
        changeDetector.findChanges();

        verify(bus, times(1)).publish(eventCaptor.capture());
        assertThat(eventCaptor.getAllValues()).hasSize(1);
        assertThat(eventCaptor.getValue()).isInstanceOf(GiosInstallation.class);
        AppEvent<GiosInstallationPayload> capturedEvent = (AppEvent<GiosInstallationPayload>) eventCaptor.getValue();
        assertThat(capturedEvent.payload).isNotNull();
        assertThat(capturedEvent.payload.type).isSameAs(GiosInstallationEventType.UPDATED);
        assertThat(capturedEvent.payload.installation).isEqualTo(updatedValue);
    }

    @Test
    void findChanges_withValueInStoreAndNoneValueInQuery_expectGiosInstallationDeletedEvent() {
        OffsetDateTime timestamp = OffsetDateTime.now();
        Installation value = InstallationFactory.installationWithPm10(1L, 10f, timestamp);

        store.upsert(TSFKey.from(value), value).await().atMost(DEFAULT_DURATION);

        ArgumentCaptor<AppEvent<?>> eventCaptor = ArgumentCaptor.forClass(AppEvent.class);
        changeDetector.findChanges();

        verify(bus, times(1)).publish(eventCaptor.capture());
        assertThat(eventCaptor.getAllValues()).hasSize(1);
        assertThat(eventCaptor.getValue()).isInstanceOf(GiosInstallation.class);
        AppEvent<GiosInstallationPayload> capturedEvent = (AppEvent<GiosInstallationPayload>) eventCaptor.getValue();
        assertThat(capturedEvent.payload).isNotNull();
        assertThat(capturedEvent.payload.type).isSameAs(GiosInstallationEventType.DELETED);
        assertThat(capturedEvent.payload.installation).isEqualTo(value);
    }
}
