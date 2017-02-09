package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ClusterTest {
    @Test
    public void testClusterCreateWithAddress() throws URISyntaxException {
        List<URI> target = new ArrayList<>(1);
        target.add(new URI("http://localhost:3000"));
        Cluster c = new Cluster(new URI("http://localhost:3000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterAddHost() throws URISyntaxException {
        List<URI> target = new ArrayList<>(1);
        target.add(new URI("http://localhost:3000"));
        Cluster c = new Cluster();
        c.addAddress(new URI("http://localhost:3000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterRemoveHost() throws URISyntaxException {
        List<URI> target = new ArrayList<>();
        Cluster c = new Cluster(new URI("localhost:5000"));
        c.removeAddress(new URI("localhost:5000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterNextHost() throws URISyntaxException {
        URI target1 = new URI("db1.pilosa.com");
        URI target2 = new URI("db2.pilosa.com");

        Cluster c = new Cluster();
        c.addAddress(new URI("db1.pilosa.com"));
        c.addAddress(new URI("db2.pilosa.com"));
        URI host = c.nextAddress();
        assertEquals(target1, host);
        host = c.nextAddress();
        assertEquals(target2, host);
        host = c.nextAddress();
        assertEquals(target1, host);
        c.removeAddress(new URI("db2.pilosa.com"));
        assertEquals(target1, host);
    }
}
