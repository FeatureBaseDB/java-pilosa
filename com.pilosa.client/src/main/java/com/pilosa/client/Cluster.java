package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains hosts in a Pilosa cluster.
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
     * @param uri address of the first host
     * @return a Cluster with the given host
     */
    public static Cluster withHost(URI uri) {
        Cluster cluster = new Cluster();
        cluster.addHost(uri);
        return cluster;
    }

    /**
     * Adds a host to the cluster.
     * @param uri Address of a Pilosa host
     */
    public synchronized void addHost(URI uri) {
        this.hosts.add(uri);
    }

    /**
     * Remove the host with the given URI from the cluster.
     *
     * @param uri of the host to be removed
     */
    public synchronized void removeHost(URI uri) {
        this.hosts.remove(uri);
    }

    /**
     * Return the next host in the cluster.
     *
     * @return next host
     */
    public synchronized URI getHost() {
        if (this.hosts.size() == 0) {
            throw new PilosaException("There are no available hosts");
        }
        URI nextHost = this.hosts.get(this.nextIndex % this.hosts.size());
        this.nextIndex = (this.nextIndex + 1) % this.hosts.size();
        return nextHost;
    }

    /**
     * Return all hosts in the cluster
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
