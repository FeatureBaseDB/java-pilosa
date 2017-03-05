package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.internal.Internal;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class PilosaClientTest {
    @Test(expected = PilosaException.class)
    public void fetchFrameNodesTest() {
        PilosaClient client = new PilosaClient("non-existent-domain-555.com:19000");
        client.fetchFrameNodes("foo", 0);
    }

    @Test(expected = PilosaException.class)
    public void importNodeTest() {
        PilosaClient client = new PilosaClient("non-existent-domain-555.com:19000");
        Internal.ImportRequest request = Internal.ImportRequest.newBuilder().build();
        client.importNode(request);
    }
}
