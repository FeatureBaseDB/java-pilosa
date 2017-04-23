package com.pilosa.client.orm;

public class PqlBaseQuery implements PqlQuery {
    private String pql;
    private Index index = null;

    PqlBaseQuery(String pql) {
        this(pql, null);
    }

    PqlBaseQuery(String pql, Index index) {
        this.pql = pql;
        this.index = index;
    }

    public Index getIndex() {
        return this.index;
    }

    public String serialize() {
        return this.pql;
    }
}
