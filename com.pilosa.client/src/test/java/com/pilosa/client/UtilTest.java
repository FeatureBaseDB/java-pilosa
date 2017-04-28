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

import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.pilosa.client.Util.protobufAttrsToMap;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class UtilTest {
    @Test
    public void protobufAttrsToMapTest() {
        List<Internal.Attr> attrs = new ArrayList<>(3);
        attrs.add(Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_STRING_TYPE)
                .setKey("stringval")
                .setStringValue("somestr")
                .build());
        attrs.add(Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_INT_TYPE)
                .setKey("intval")
                .setIntValue(5)
                .build());
        attrs.add(Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_BOOL_TYPE)
                .setKey("boolval")
                .setBoolValue(true)
                .build());
        attrs.add(Internal.Attr.newBuilder()
                .setKey("doubleval")
                .setType(Util.PROTOBUF_DOUBLE_TYPE)
                .setFloatValue(123.5678)
                .build());
        Map<String, Object> m = protobufAttrsToMap(attrs);
        assertEquals(4, m.size());
        assertEquals("somestr", m.get("stringval"));
        assertEquals(5L, m.get("intval"));
        assertEquals(true, m.get("boolval"));
        assertEquals(123.5678, m.get("doubleval"));
    }

    @Test(expected = PilosaException.class)
    public void protobufAttrsToMapFailsTest() {
        List<Internal.Attr> attrs = new ArrayList<>(3);
        attrs.add(Internal.Attr.newBuilder()
                .setType(9)
                .setKey("stringval")
                .setStringValue("somestr")
                .build());
        protobufAttrsToMap(attrs);
    }

    @Test
    public void createUtilTest() {
        // this test is required only to get 100% coverage
        new Util();
    }
}
