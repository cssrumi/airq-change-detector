package pl.airq.detector.change.config;

import io.quarkus.arc.config.ConfigProperties;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import pl.airq.common.config.properties.ExpireIn;

@ConfigProperties(prefix = "change-detector")
public class ChangeDetectorProperties {

    @NotNull
    private Detect detect;
    @NotNull
    private Loader loader;
    @NotNull
    private Store store;

    public Detect getDetect() {
        return detect;
    }

    public void setDetect(Detect detect) {
        this.detect = detect;
    }

    public Loader getLoader() {
        return loader;
    }

    public void setLoader(Loader loader) {
        this.loader = loader;
    }

    public Store getStore() {
        return store;
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public static class Detect {

        @Positive
        private Integer sinceLast;
        @NotNull
        private ChronoUnit timeUnit;
        @Positive
        private Integer maxProcessAwait;
        @NotNull
        private ChronoUnit maxProcessAwaitTimeUnit;

        public Duration sinceLastDuration() {
            return Duration.of(sinceLast, timeUnit);
        }

        public Duration maxProcessAwaitDuration() {
            return Duration.of(maxProcessAwait, maxProcessAwaitTimeUnit);
        }

        public Integer getSinceLast() {
            return sinceLast;
        }

        public void setSinceLast(Integer sinceLast) {
            this.sinceLast = sinceLast;
        }

        public ChronoUnit getTimeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(ChronoUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public Integer getMaxProcessAwait() {
            return maxProcessAwait;
        }

        public void setMaxProcessAwait(Integer maxProcessAwait) {
            this.maxProcessAwait = maxProcessAwait;
        }

        public ChronoUnit getMaxProcessAwaitTimeUnit() {
            return maxProcessAwaitTimeUnit;
        }

        public void setMaxProcessAwaitTimeUnit(ChronoUnit maxProcessAwaitTimeUnit) {
            this.maxProcessAwaitTimeUnit = maxProcessAwaitTimeUnit;
        }
    }

    public static class Loader {

        @NotNull
        private Boolean pullFromDb;
        @Positive
        private Long maxAwaitInSeconds = 60L;

        public Boolean getPullFromDb() {
            return pullFromDb;
        }

        public void setPullFromDb(Boolean pullFromDb) {
            this.pullFromDb = pullFromDb;
        }

        public Long getMaxAwaitInSeconds() {
            return maxAwaitInSeconds;
        }

        public void setMaxAwaitInSeconds(Long maxAwaitInSeconds) {
            this.maxAwaitInSeconds = maxAwaitInSeconds;
        }
    }

    public static class Store {

        @NotNull
        private ExpireIn expire;

        public ExpireIn getExpire() {
            return expire;
        }

        public void setExpire(ExpireIn expire) {
            this.expire = expire;
        }
    }
}
