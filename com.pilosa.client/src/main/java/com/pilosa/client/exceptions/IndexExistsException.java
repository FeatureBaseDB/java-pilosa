package com.pilosa.client.exceptions;

public class IndexExistsException extends PilosaException {
    public IndexExistsException() {
        super("Index already exists");
    }
}
