package ai.logstream.worker.infer;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;

import java.io.IOException;

public class BearerAuthFilter implements ClientRequestFilter {

    private final String token;

    public BearerAuthFilter(String token) {
        this.token = token;
    }

    @Override
    public void filter(ClientRequestContext requestContext) throws IOException {
        requestContext.getHeaders().putSingle("Authorization", "Bearer " + token);
    }
}
