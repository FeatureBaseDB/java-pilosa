package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains hosts in a Pilosa cluster.
 */

public final class Cluster {
    private List<URI> hosts = new ArrayList<>();
    private int nextIndex = 0;

    /**
     * Creates the default cluster.
     */
    public Cluster() {
    }

    /**
     * Adds a host to the cluster.
     *
     * @param address Address of a Pilosa host
     */
    public synchronized void addHost(URI address) {
        this.hosts.add(address);
    }

    public synchronized void removeHost(URI address) {
        this.hosts.remove(address);
    }

    public synchronized URI getHost() {
        if (this.hosts.size() == 0) {
            throw new PilosaException("There are no available hosts");
        }
        URI nextHost = this.hosts.get(this.nextIndex % this.hosts.size());
        this.nextIndex = (this.nextIndex + 1) % this.hosts.size();
        return nextHost;
    }

    List<URI> getHosts() {
        return this.hosts;
    }
}
