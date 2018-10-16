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

package com.pilosa.client.csv;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.Record;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public abstract class LineUnserializer {
    abstract Record unserialize(String[] fields);

    public void setTimestampFormat(SimpleDateFormat format) {
        this.timestampFormat = format;
        if (this.timestampFormat != null) {
            this.timestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }
    }

    protected long parseTimestamp(final String s) {
        if (this.timestampFormat == null) {
            return Long.parseLong(s);
        }
        try {
            Date date = this.timestampFormat.parse(s);
            return date.getTime() / 1000;
        } catch (ParseException ex) {
            throw new PilosaException(String.format("Error parsing timestamp: %s", s), ex);
        }
    }

    public LineUnserializer() {
        this.setTimestampFormat(defaultTimestampFormat);
    }

    protected final static SimpleDateFormat defaultTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
    protected SimpleDateFormat timestampFormat = null;
}
