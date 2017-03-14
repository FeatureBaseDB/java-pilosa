package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains addresses of nodes in a Pilosa cluster.
 */

public final class Cluster implements ICluster {
    private List<URI> addresses = new ArrayList<>();
    private int nextIndex = 0;

    /**
     * Creates the default cluster.
     */
    public Cluster() {
    }

    /**
     * Creates the cluster from a Pilosa address.
     *
     * @param address Pilosa address
     */
    Cluster(URI address) {
        this.addAddress(address);
    }

    /**
     * Adds an address to the cluster.
     *
     * @param address Pilosa address
     */
    public void addAddress(URI address) {
        this.addresses.add(address);
    }

    public void removeAddress(URI address) {
        this.addresses.remove(address);
    }

    public URI getAddress() {
        if (this.addresses.size() == 0) {
            throw new PilosaException("There are no available addresses");
        }
        URI nextHost = this.addresses.get(this.nextIndex % this.addresses.size());
        this.nextIndex = (this.nextIndex + 1) % this.addresses.size();
        return nextHost;
    }

    List<URI> getAddresses() {
        return this.addresses;
    }
}
