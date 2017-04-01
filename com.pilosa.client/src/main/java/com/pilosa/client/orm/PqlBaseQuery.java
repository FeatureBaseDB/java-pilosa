package com.pilosa.client.orm;

public class PqlBaseQuery implements PqlQuery {
    private String pql;
    private Database database = null;

    PqlBaseQuery(String pql) {
        this(pql, null);
    }

    PqlBaseQuery(String pql, Database database) {
        this.pql = pql;
        this.database = database;
    }

    public Database getDatabase() {
        return this.database;
    }

    @Override
    public String toString() {
        return this.pql;
    }

}
