package com.pilosa.client;

public interface ICluster {
    URI getAddress();

    void removeAddress(URI address);
}
