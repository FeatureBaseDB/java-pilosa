/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

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
