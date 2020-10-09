package pl.airq.detector.change;

import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.annotations.jaxrs.PathParam;
import pl.airq.common.domain.gios.installation.InstallationQuery;
import pl.airq.common.store.key.TSFKey;

@Path("/query")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TestEndpoint {

    private final InstallationQuery installationQuery;

    @Inject
    public TestEndpoint(InstallationQuery installationQuery) {
        this.installationQuery = installationQuery;
    }

    @GET
    @Path("/installations/first")
    public Uni<Response> first() {
        return installationQuery.getAllWithPMSinceLastNHours(10)
                                .map(Set::stream)
                                .map(Stream::findFirst)
                                .map(installation -> installation.orElse(null))
                                .map(TSFKey::from)
                                .flatMap(installationQuery::getByTSFKey)
                                .map(installation -> Response.ok(installation).build());
    }

    @GET
    @Path("/installations/{n}")
    public Response findAllSinceNHours(@PathParam Integer n) {
        return installationQuery.getAllWithPMSinceLastNHours(n)
                                .map(Response::ok)
                                .map(Response.ResponseBuilder::build)
                                .await().atMost(Duration.ofSeconds(6));
    }
}
