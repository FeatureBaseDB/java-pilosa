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

package com.pilosa.client.orm;

import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.PilosaException;
import org.apache.commons.lang3.builder.HashCodeBuilder;

class RangeField {
    @Override
    public String toString() {
        return String.format("{\"name\":\"%s\",\"type\":\"%s\",\"min\":%d,\"max\":%d}",
                this.name, this.type, this.min, this.max);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RangeField)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        RangeField rhs = (RangeField) obj;
        return rhs.name.equals(this.name) &&
                rhs.type.equals(this.type) &&
                rhs.min == this.min &&
                rhs.max == this.max;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.name)
                .append(this.type)
                .append(this.min)
                .append(this.max)
                .toHashCode();
    }

    RangeField(final String name, final String type, final long min, final long max) {
        Validator.ensureValidLabel(name);
        if (max <= min) {
            throw new PilosaException("`max` should be greater than `min` for frame option field: " + name);
        }
        this.name = name;
        this.type = type;
        this.min = min;
        this.max = max;
    }

    private final String name;
    private final String type;
    private final long min;
    private final long max;
}
