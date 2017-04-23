package com.pilosa.client.orm;

public class PqlBitmapQuery extends PqlBaseQuery {
    PqlBitmapQuery(String pql) {
        super(pql);
    }

    PqlBitmapQuery(String pql, Index index) {
        super(pql, index);
    }
}
