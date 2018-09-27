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

import com.pilosa.client.orm.Field;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import static com.pilosa.client.PilosaClient.PQL_VERSION;

class ImportRequest {
    ImportRequest(final String path, final byte[] payload, final String contentType) {
        this.path = path;
        this.payload = payload;
        this.contentType = contentType;
    }

    static ImportRequest createCSVImport(final Field field, final byte[] payload) {
        String path = String.format("/index/%s/field/%s/import", field.getIndex().getName(), field.getName());
        return new ImportRequest(path, payload, "application/x-protobuf");
    }

    static ImportRequest createRoaringImport(final Field field, long shard, final byte[] payload) {
        String path = String.format("/index/%s/field/%s/import-roaring/%d",
                field.getIndex().getName(), field.getName(), shard);
        return new ImportRequest(path, payload, "application/x-binary");
    }

    String getPath() {
        return this.path;
    }

    byte[] getPayload() {
        return this.payload;
    }

    Header[] getHeaders() {
        return new Header[]{
                new BasicHeader("Content-Type", this.contentType),
                new BasicHeader("Accept", "application/x-protobuf"),
                new BasicHeader("PQL-Version", PQL_VERSION)
        };
    }

    protected final String path;
    protected final String contentType;
    protected final byte[] payload;
}
