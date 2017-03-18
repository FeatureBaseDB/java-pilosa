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
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterAddHost() {
        List<URI> target = new ArrayList<>(1);
        target.add(new URI("http://localhost:3000"));
        Cluster c = new Cluster();
        c.addHost(new URI("http://localhost:3000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterRemoveHost() {
        List<URI> target = new ArrayList<>();
        Cluster c = new Cluster(new URI("localhost:5000"));
        c.removeHost(new URI("localhost:5000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterGetHost() {
        URI target1 = new URI("db1.pilosa.com");
        URI target2 = new URI("db2.pilosa.com");

        Cluster c = new Cluster();
        c.addHost(new URI("db1.pilosa.com"));
        c.addHost(new URI("db2.pilosa.com"));
        URI addr = c.getHost();
        assertEquals(target1, addr);
        addr = c.getHost();
        assertEquals(target2, addr);
        c.getHost();
        c.removeHost(new URI("db1.pilosa.com"));
        addr = c.getHost();
        assertEquals(target2, addr);
    }

    @Test(expected = PilosaException.class)
    public void testClusterGetHostWitEmptyList() {
        Cluster c = new Cluster();
        c.getHost();
    }
}
