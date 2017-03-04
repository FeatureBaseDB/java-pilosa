package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;

import java.util.regex.Pattern;

final class Validator {
    // See: https://github.com/pilosa/pilosa/issues/280
    private final static Pattern DATABASE_NAME = Pattern.compile("^[a-z0-9_-]+$");
    private final static Pattern FRAME_NAME = Pattern.compile("^[.a-z0-9_-]+$");
    private final static int MAX_DATABASE_NAME = 64;
    private final static int MAX_FRAME_NAME = 64;

    Validator() {}

    static boolean validateDatabaseName(String databaseName) {
        //noinspection SimplifiableIfStatement
        if (databaseName.length() > MAX_DATABASE_NAME) {
            return false;
        }
        return DATABASE_NAME.matcher(databaseName).matches();
    }

    static void ensureValidDatabaseName(String databaseName) {
        if (!validateDatabaseName(databaseName)) {
            throw new ValidationException(String.format("Invalid database name: %s", databaseName));
        }
    }

    static boolean validateFrameName(String frameName) {
        //noinspection SimplifiableIfStatement
        if (frameName.length() > MAX_FRAME_NAME) {
            return false;
        }
        return FRAME_NAME.matcher(frameName).matches();
    }

    static void ensureValidFrameName(String frameName) {
        if (!validateFrameName(frameName)) {
            throw new ValidationException(String.format("Invalid frame name: %s", frameName));
        }
    }
}
