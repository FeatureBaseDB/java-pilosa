package com.pilosa.client.exceptions;

public class DatabaseExistsException extends PilosaException {
    public DatabaseExistsException() {
        super("Database already exists");
    }
}
