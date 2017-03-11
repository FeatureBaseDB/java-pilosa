package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;

import java.util.regex.Pattern;

public final class Validator {
    // See: https://github.com/pilosa/pilosa/issues/280
    private final static Pattern DATABASE_NAME = Pattern.compile("^[a-z0-9_-]+$");
    private final static Pattern FRAME_NAME = Pattern.compile("^[a-z0-9][.a-z0-9_-]*$");
    private final static Pattern LABEL = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    private final static int MAX_DATABASE_NAME = 64;
    private final static int MAX_FRAME_NAME = 64;
    private final static int MAX_LABEL = 64;

    Validator() {}

    public static boolean validateDatabaseName(String databaseName) {
        //noinspection SimplifiableIfStatement
        if (databaseName.length() > MAX_DATABASE_NAME) {
            return false;
        }
        return DATABASE_NAME.matcher(databaseName).matches();
    }

    public static void ensureValidDatabaseName(String databaseName) {
        if (!validateDatabaseName(databaseName)) {
            throw new ValidationException(String.format("Invalid database name: %s", databaseName));
        }
    }

    public static boolean validateFrameName(String frameName) {
        //noinspection SimplifiableIfStatement
        if (frameName.length() > MAX_FRAME_NAME) {
            return false;
        }
        return FRAME_NAME.matcher(frameName).matches();
    }

    public static void ensureValidFrameName(String frameName) {
        if (!validateFrameName(frameName)) {
            throw new ValidationException(String.format("Invalid frame name: %s", frameName));
        }
    }

    public static boolean validateLabel(String label) {
        //noinspection SimplifiableIfStatement
        if (label.length() > MAX_LABEL) {
            return false;
        }
        return LABEL.matcher(label).matches();
    }

    public static void ensureValidLabel(String label) {
        if (!validateLabel(label)) {
            throw new ValidationException(String.format("Invalid label: %s", label));
        }
    }
}
