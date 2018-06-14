/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;

import java.util.regex.Pattern;

public final class Validator {
    // See: https://github.com/pilosa/pilosa/issues/280
    private final static Pattern INDEX_NAME = Pattern.compile("^[a-z][a-z0-9_-]*$");
    private final static Pattern FIELD_NAME = Pattern.compile("^[a-z][a-z0-9_-]*$");
    private final static Pattern LABEL = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_-]*$");
    private final static Pattern KEY = Pattern.compile("^[A-Za-z0-9_{}+/=.~%:-]*$");
    private final static int MAX_INDEX_NAME = 64;
    private final static int MAX_FIELD_NAME = 64;
    private final static int MAX_LABEL = 64;
    private final static int MAX_KEY = 64;

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
    public static boolean validFieldName(String fieldName) {
        //noinspection SimplifiableIfStatement
        if (fieldName.length() > MAX_FIELD_NAME) {
            return false;
        }
        return FIELD_NAME.matcher(fieldName).matches();
    }

    public static void ensureValidFieldName(String fieldName) {
        if (!validFieldName(fieldName)) {
            throw new ValidationException(String.format("Invalid field name: %s", fieldName));
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

    @SuppressWarnings("WeakerAccess")
    public static boolean validKey(String key) {
        //noinspection SimplifiableIfStatement
        if (key.length() > MAX_KEY) {
            return false;
        }
        return KEY.matcher(key).matches();
    }

    public static void ensureValidKey(String key) {
        if (!validKey(key)) {
            throw new ValidationException(String.format("Invalid key: %s", key));
        }
    }
}
