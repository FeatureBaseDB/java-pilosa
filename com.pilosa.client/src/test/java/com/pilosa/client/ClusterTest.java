package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ClusterTest {
    @Test
    public void testClusterCreateWithAddress() {
        List<URI> target = new ArrayList<>(1);
        target.add(new URI("http://localhost:3000"));
        Cluster c = new Cluster(new URI("http://localhost:3000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterAddAddress() {
        List<URI> target = new ArrayList<>(1);
        target.add(new URI("http://localhost:3000"));
        Cluster c = new Cluster();
        c.addAddress(new URI("http://localhost:3000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterRemoveAddress() {
        List<URI> target = new ArrayList<>();
        Cluster c = new Cluster(new URI("localhost:5000"));
        c.removeAddress(new URI("localhost:5000"));
        assertEquals(target, c.getAddresses());
    }

    @Test
    public void testClusterGetAddress() {
        URI target1 = new URI("db1.pilosa.com");
        URI target2 = new URI("db2.pilosa.com");

        Cluster c = new Cluster();
        c.addAddress(new URI("db1.pilosa.com"));
        c.addAddress(new URI("db2.pilosa.com"));
        URI addr = c.getAddress();
        assertEquals(target1, addr);
        addr = c.getAddress();
        assertEquals(target2, addr);
        addr = c.getAddress();
        assertEquals(target1, addr);
        c.removeAddress(new URI("db2.pilosa.com"));
        assertEquals(target1, addr);
    }

    @Test(expected = PilosaException.class)
    public void testClusterGetAddressWitEmptyList() {
        Cluster c = new Cluster();
        c.getAddress();
    }
}
