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
        target.add(URI.address("http://localhost:3000"));
        Cluster c = Cluster.withHost(URI.address("http://localhost:3000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterAddRemoveHost() {
        List<URI> target = new ArrayList<>(1);
        target.add(URI.address("http://localhost:3000"));
        Cluster c = Cluster.defaultCluster();
        c.addHost(URI.address("http://localhost:3000"));
        assertEquals(target, c.getHosts());

        target = new ArrayList<>(2);
        target.add(URI.address("http://localhost:3000"));
        target.add(URI.defaultURI());
        c.addHost(URI.defaultURI());
        assertEquals(target, c.getHosts());

        target = new ArrayList<>(1);
        target.add(URI.defaultURI());
        c.removeHost(URI.address("localhost:3000"));
        assertEquals(target, c.getHosts());
    }

    @Test
    public void testClusterGetHost() {
        URI target1 = URI.address("db1.pilosa.com");
        URI target2 = URI.address("db2.pilosa.com");

        Cluster c = Cluster.defaultCluster();
        c.addHost(URI.address("db1.pilosa.com"));
        c.addHost(URI.address("db2.pilosa.com"));
        URI addr = c.getHost();
        assertEquals(target1, addr);
        addr = c.getHost();
        assertEquals(target2, addr);
        c.getHost();
        c.removeHost(URI.address("db1.pilosa.com"));
        addr = c.getHost();
        assertEquals(target2, addr);
    }

    @Test(expected = PilosaException.class)
    public void testClusterGetHostWhenNoHosts() {
        Cluster c = Cluster.defaultCluster();
        c.getHost();
    }
}
