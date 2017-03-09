package com.pilosa.client.orm;

public class PqlBitmapQuery extends PqlQuery {
    PqlBitmapQuery(String pql) {
        super(pql);
    }

    PqlBitmapQuery(String pql, Database database) {
        super(pql, database);
    }
}
