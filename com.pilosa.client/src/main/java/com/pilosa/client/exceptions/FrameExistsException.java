package com.pilosa.client.exceptions;

public class FrameExistsException extends PilosaException {
    public FrameExistsException() {
        super("Frame already exists");
    }
}
