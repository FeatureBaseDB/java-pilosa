package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Bit {
    private Internal.Bit iBit = null;

    private Bit() {
    }

    public static Bit create(long bitmapID, long profileID) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setBitmapID(bitmapID)
                .setProfileID(profileID)
                .build();
        return bit;
    }

    public static Bit create(long bitmapID, long profileID, long timestamp) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setBitmapID(bitmapID)
                .setProfileID(profileID)
                .setTimestamp(timestamp)
                .build();
        return bit;
    }

    public long getBitmapID() {
        return this.iBit.getBitmapID();
    }

    public long getProfileID() {
        return this.iBit.getProfileID();
    }

    public long getTimestamp() {
        return this.iBit.getTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Bit)) {
            return false;
        }

        Bit bit = (Bit) o;
        return this.iBit.equals(bit.iBit);
    }

    @Override
    public int hashCode() {
        return this.iBit.hashCode();
    }
}
