package com.pilosa.client;

public class PilosaException extends RuntimeException {
    public PilosaException() {
    }

    public PilosaException(String message) {
        super(message);
    }

    public PilosaException(String message, Throwable t) {
        super(message, t);
    }
}
