package com.pilosa.client.orm;

public interface PqlQuery {
    Database getDatabase();

    String serialize();
}
