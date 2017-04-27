package com.pilosa.client.orm;

public interface PqlQuery {
    /**
     * Returns the index of this query.
     *
     * @return index of this query
     */
    Index getIndex();

    /**
     * @return the query in a form consumable by <code>PilosaClient</code>
     */
    String serialize();
}
