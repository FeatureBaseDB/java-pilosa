package com.pilosa.client.orm;

public interface PqlQuery {
    Index getIndex();
    String serialize();
}
