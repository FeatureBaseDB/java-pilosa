package com.pilosa.client;

public class Bit {
    private Internal.Bit iBit = null;

    private Bit() {
    }

    public static Bit create(long rowID, long columnID) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setRowID(rowID)
                .setColumnID(columnID)
                .build();
        return bit;
    }

    public static Bit create(long rowID, long columnID, long timestamp) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setRowID(rowID)
                .setColumnID(columnID)
                .setTimestamp(timestamp)
                .build();
        return bit;
    }

    public long getRowID() {
        return this.iBit.getRowID();
    }

    public long getColumnID() {
        return this.iBit.getColumnID();
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
