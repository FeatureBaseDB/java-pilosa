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
     * Returns a cluster with the given URIs.
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
     * Makes a host available.
     *
     * @param uri Address of a Pilosa host
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void addHost(URI uri) {
        int index = this.hosts.indexOf(uri);
        if (index >= 0) {
            this.okList.set(index, true);
        } else {
            this.hosts.add(uri);
            this.okList.add(true);
        }
    }

    /**
     * Makes a host unavailable.
     *
     * @param uri of the host to be removed
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void removeHost(URI uri) {
        int index = this.hosts.indexOf(uri);
        if (index >= 0) {
            this.okList.set(index, false);
        }
    }

    /**
     * Returns the first available host in the cluster.
     *
     * @return host
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized URI getHost() {
        int index = this.okList.indexOf(true);
        if (index < 0) {
            reset();
            throw new PilosaException("There are no available hosts");
        }
        return this.hosts.get(index);
    }

    /**
     * Returns available hosts in the cluster.
     *
     * @return available hosts
     */
    List<URI> getHosts() {
        List<URI> hosts = new ArrayList<>();
        for (int i = 0; i < this.okList.size(); i++) {
            if (this.okList.get(i)) {
                hosts.add(this.hosts.get(i));
            }
        }
        return hosts;
    }

    void reset() {
        for (int i = 0; i < this.okList.size(); i++) {
            this.okList.set(i, true);
        }
    }

    private Cluster() {
    }

    private List<URI> hosts = new ArrayList<>();
    private List<Boolean> okList = new ArrayList<>();
}
