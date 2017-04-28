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

import java.util.ArrayList;
import java.util.List;

/**
 * Contains hosts in a Pilosa cluster.
 *
 * @see <a href="https://www.pilosa.com/docs/configuration/#all-options">Pilosa Configuration: All Options</a>
 */

public final class Cluster {
    /**
     * Returns the default cluster.
     *
     * @return the default cluster
     */
    public static Cluster defaultCluster() {
        return new Cluster();
    }

    /**
     * Returns a cluster with the given URI.
     *
     * @param uris addresses of hosts
     * @return a Cluster with the given host
     */
    @SuppressWarnings("WeakerAccess")
    public static Cluster withHost(URI... uris) {
        Cluster cluster = new Cluster();
        for (URI uri : uris) {
            cluster.addHost(uri);
        }
        return cluster;
    }

    /**
     * Adds a host to the cluster.
     *
     * @param uri Address of a Pilosa host
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void addHost(URI uri) {
        this.hosts.add(uri);
    }

    /**
     * Removes the host with the given URI from the cluster.
     *
     * @param uri of the host to be removed
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void removeHost(URI uri) {
        this.hosts.remove(uri);
    }

    /**
     * Returns the next host in the cluster.
     *
     * @return next host
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized URI getHost() {
        if (this.hosts.size() == 0) {
            throw new PilosaException("There are no available hosts");
        }
        URI nextHost = this.hosts.get(this.nextIndex % this.hosts.size());
        this.nextIndex = (this.nextIndex + 1) % this.hosts.size();
        return nextHost;
    }

    /**
     * Returns all hosts in the cluster.
     *
     * @return all hosts
     */
    List<URI> getHosts() {
        return this.hosts;
    }

    private Cluster() {
    }

    private List<URI> hosts = new ArrayList<>();
    private int nextIndex = 0;
}
