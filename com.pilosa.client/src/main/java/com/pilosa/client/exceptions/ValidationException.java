package com.pilosa.client.exceptions;

public class ValidationException extends PilosaException {
    public ValidationException() {
        super();
    }

    public ValidationException(String message) {
        super(message);
    }
}
