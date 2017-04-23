package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;

import java.util.regex.Pattern;

public final class Validator {
    // See: https://github.com/pilosa/pilosa/issues/280
    private final static Pattern INDEX_NAME = Pattern.compile("^[a-z0-9_-]+$");
    private final static Pattern FRAME_NAME = Pattern.compile("^[a-z0-9][.a-z0-9_-]*$");
    private final static Pattern LABEL = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    private final static int MAX_INDEX_NAME = 64;
    private final static int MAX_FRAME_NAME = 64;
    private final static int MAX_LABEL = 64;

    Validator() {}

    @SuppressWarnings("WeakerAccess")
    public static boolean validIndexName(String indexName) {
        //noinspection SimplifiableIfStatement
        if (indexName.length() > MAX_INDEX_NAME) {
            return false;
        }
        return INDEX_NAME.matcher(indexName).matches();
    }

    public static void ensureValidIndexName(String indexName) {
        if (!validIndexName(indexName)) {
            throw new ValidationException(String.format("Invalid index name: %s", indexName));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static boolean validFrameName(String frameName) {
        //noinspection SimplifiableIfStatement
        if (frameName.length() > MAX_FRAME_NAME) {
            return false;
        }
        return FRAME_NAME.matcher(frameName).matches();
    }

    public static void ensureValidFrameName(String frameName) {
        if (!validFrameName(frameName)) {
            throw new ValidationException(String.format("Invalid frame name: %s", frameName));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static boolean validLabel(String label) {
        //noinspection SimplifiableIfStatement
        if (label.length() > MAX_LABEL) {
            return false;
        }
        return LABEL.matcher(label).matches();
    }

    public static void ensureValidLabel(String label) {
        if (!validLabel(label)) {
            throw new ValidationException(String.format("Invalid label: %s", label));
        }
    }
}
