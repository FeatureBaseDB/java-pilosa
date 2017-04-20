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
