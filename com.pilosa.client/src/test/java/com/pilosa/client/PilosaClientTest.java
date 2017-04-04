package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

@Category(UnitTest.class)
public class PilosaClientTest {
    @Test
    public void defaultClientTest() throws IOException {
        try (PilosaClient client = PilosaClient.defaultClient()) {
            assertNotNull(client);
        }
    }

    // Note that following tests need access to internal methods, that's why they are here.
    @Test(expected = PilosaException.class)
    public void fetchFrameNodesTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("non-existent-domain-555.com:19000")) {
            client.fetchFrameNodes("foo", 0);
        }
    }

    @Test(expected = PilosaException.class)
    public void importNodeTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("non-existent-domain-555.com:19000")) {
            Internal.ImportRequest request = Internal.ImportRequest.newBuilder().build();
            client.importNode(request);
        }
    }
}
