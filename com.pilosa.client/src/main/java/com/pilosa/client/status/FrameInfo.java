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

package com.pilosa.client.status;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pilosa.client.TimeQuantum;
import com.pilosa.client.orm.CacheType;
import com.pilosa.client.orm.FrameOptions;

public class FrameInfo implements IFrameInfo {
    public FrameOptions getOptions() {
        return this.meta.getOptions();
    }

    @JsonProperty("name")
    public String getName() {
        return this.name;
    }

    void setName(String name) {
        this.name = name;
    }

    @JsonProperty("options")
    void setMeta(FrameMeta meta) {
        this.meta = meta;
    }

    private String name;
    private FrameMeta meta = new FrameMeta();
}

final class FrameMeta {
    FrameOptions getOptions() {
        return FrameOptions.builder()
                .setInverseEnabled(this.inverseEnabled)
                .setTimeQuantum(this.timeQuantum)
                .setCacheType(this.cacheType)
                .setCacheSize(this.cacheSize)
                .build();
    }

    @JsonProperty("timeQuantum")
    void setTimeQuantum(String s) {
        this.timeQuantum = TimeQuantum.fromString(s);
    }

    @JsonProperty("inverseEnabled")
    void setInverseEnabled(boolean inverseEnabled) {
        this.inverseEnabled = inverseEnabled;
    }

    @JsonProperty("cacheType")
    void setCacheType(String s) {
        this.cacheType = CacheType.fromString(s);
    }

    @JsonProperty("cacheSize")
    void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    private TimeQuantum timeQuantum = TimeQuantum.NONE;
    private boolean inverseEnabled = false;
    private CacheType cacheType = CacheType.DEFAULT;
    private int cacheSize = 0;
}
