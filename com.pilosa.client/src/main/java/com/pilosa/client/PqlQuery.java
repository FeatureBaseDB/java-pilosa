package com.pilosa.client;

public class PqlQuery {
    private String pql;

    PqlQuery(String pql) {
        this.pql = pql;
    }

    @Override
    public String toString() {
        return this.pql;
    }

}
