package com.pilosa.client;

/**
 * Contains addresses of nodes in a Pilosa cluster.
 */
public interface ICluster {
    /**
     * Returns a Pilosa server address.
     *
     * @return Pilosa server address
     */
    URI getAddress();

    /**
     * Discards a Pilosa address.
     * <p>
     * Depending on the class, this method may remove an address
     * or blacklist it for some time.
     * </p>
     *
     * @param address a Pilosa address
     */
    void removeAddress(URI address);
}
