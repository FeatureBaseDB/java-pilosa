package com.pilosa.client.orm;

public interface PqlQuery {
    Index getIndex();

    /**
     * @return the query in a form consumable by {@link com.pilosa.client.PilosaClient}
     */
    String serialize();
}
