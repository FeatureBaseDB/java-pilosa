package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.internal.ClientProtos;
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
        List<ClientProtos.Attr> attrs = new ArrayList<>(3);
        attrs.add(ClientProtos.Attr.newBuilder()
                .setType(ClientProtos.Attr.STRINGVALUE_FIELD_NUMBER)
                .setKey("stringval")
                .setStringValue("somestr")
                .build());
        attrs.add(ClientProtos.Attr.newBuilder()
                .setType(ClientProtos.Attr.UINTVALUE_FIELD_NUMBER)
                .setKey("intval")
                .setUintValue(5)
                .build());
        attrs.add(ClientProtos.Attr.newBuilder()
                .setType(ClientProtos.Attr.BOOLVALUE_FIELD_NUMBER)
                .setKey("boolval")
                .setBoolValue(true)
                .build());
        Map<String, Object> m = protobufAttrsToMap(attrs);
        assertEquals(3, m.size());
        assertEquals("somestr", m.get("stringval"));
        assertEquals(5L, m.get("intval"));
        assertEquals(true, m.get("boolval"));
    }

    @Test(expected = PilosaException.class)
    public void protobufAttrsToMapFailsTest() {
        List<ClientProtos.Attr> attrs = new ArrayList<>(3);
        attrs.add(ClientProtos.Attr.newBuilder()
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