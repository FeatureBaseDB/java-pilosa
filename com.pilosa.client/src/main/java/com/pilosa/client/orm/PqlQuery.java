package com.pilosa.client.orm;

public class PqlQuery implements IPqlQuery {
    private String pql;
    private Database database = null;

    PqlQuery(String pql) {
        this(pql, null);
    }

    PqlQuery(String pql, Database database) {
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
