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
    public void testClusterCreateWithHost() {
        List<URI> target = new ArrayList<>(1);
        target.add(URI.fromAddress("http://localhost:3000"));
        Cluster c = Cluster.withHost(URI.fromAddress("http://localhost:3000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterAddHost() {
        List<URI> target = new ArrayList<>(1);
        target.add(URI.fromAddress("http://localhost:3000"));
        Cluster c = Cluster.defaultCluster();
        c.addHost(URI.fromAddress("http://localhost:3000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterRemoveHost() {
        List<URI> target = new ArrayList<>();
        Cluster c = Cluster.defaultCluster();
        c.addHost(URI.fromAddress("localhost:5000"));
        c.removeHost(URI.fromAddress("localhost:5000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterGetHost() {
        URI target1 = URI.fromAddress("db1.pilosa.com");
        URI target2 = URI.fromAddress("db2.pilosa.com");

        Cluster c = Cluster.defaultCluster();
        c.addHost(URI.fromAddress("db1.pilosa.com"));
        c.addHost(URI.fromAddress("db2.pilosa.com"));
        URI addr = c.getHost();
        assertEquals(target1, addr);
        addr = c.getHost();
        assertEquals(target2, addr);
        c.getHost();
        c.removeHost(URI.fromAddress("db1.pilosa.com"));
        addr = c.getHost();
        assertEquals(target2, addr);
    }

    @Test(expected = PilosaException.class)
    public void testClusterGetHostWitEmptyList() {
        Cluster c = Cluster.defaultCluster();
        c.getHost();
    }
}
