package com.pilosa.client;

import java.util.ArrayList;
import java.util.List;

public final class Cluster implements ICluster {
    private List<URI> addresses = new ArrayList<>();
    private int nextIndex = 0;

    public Cluster() {
    }

    Cluster(URI address) {
        this.addAddress(address);
    }

    public Cluster(Cluster other) {
        this.addresses = new ArrayList<>(other.addresses);
        this.nextIndex = other.nextIndex;
    }

    public void addAddress(URI address) {
        this.addresses.add(address);
    }

    public void removeAddress(URI address) {
        this.addresses.remove(address);
    }

    public URI getAddress() {
        if (this.nextIndex >= this.addresses.size()) {
            throw new PilosaException("There are no available addresses");
        }
        URI nextHost = this.addresses.get(this.nextIndex);
        this.nextIndex = (this.nextIndex + 1) % this.addresses.size();
        return nextHost;
    }

    List<URI> getAddresses() {
        return this.addresses;
    }
}
