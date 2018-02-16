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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerVersion {
    public static boolean isLegacy(String version) {
        return isLegacy(version, PILOSA_MINIMUM_VERSION);
    }

    static boolean isLegacy(String version, String serverVersion) {
        Matcher matcher = VERSION.matcher(serverVersion);
        if (!matcher.matches()) {
            throw new ValidationException(String.format("Invalid server version: %s", serverVersion));
        }
        int[] sv = new int[matcher.groupCount()];
        for (int i = 0; i < sv.length; i++) {
            sv[i] = Integer.decode(matcher.group(i + 1));
        }
        matcher = VERSION.matcher(version);
        if (!matcher.matches()) {
            return true;
        }
        if (matcher.groupCount() < sv.length) {
            throw new ValidationException(String.format("Invalid version: %s", version));
        }
        int[] v = new int[matcher.groupCount()];
        for (int i = 0; i < v.length; i++) {
            v[i] = Integer.decode(matcher.group(i + 1));
        }
        for (int i = 0; i < sv.length; i++) {
            if (sv[i] < v[i]) {
                break;
            }
            if (sv[i] > v[i]) {
                return true;
            }
        }
        return false;
    }

    private static final String PILOSA_MINIMUM_VERSION = "0.9.0";
    private final static Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+).(\\d+).*");
}
