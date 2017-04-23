package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class ColumnItemTest {
    @Test
    public void testCreateProfileItem() {
        ColumnItem pi = createSampleProfileItem();
        assertEquals(33, pi.getID());
        assertEquals("Austin", pi.getAttributes().get("city"));
    }

    @Test
    public void testCreateProfileItemDefaultConstructor() {
        new ColumnItem();
    }

    @Test
    public void testBitmapResultToString() {
        ColumnItem result = createSampleProfileItem();
        String s = result.toString();
        assertEquals("ColumnItem(id=33, attrs={city=Austin})", s);
    }

    @Test
    public void testEquals() {
        ColumnItem result1 = createSampleProfileItem();
        ColumnItem result2 = createSampleProfileItem();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new ColumnItem(1, null)).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        ColumnItem result = createSampleProfileItem();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        ColumnItem result1 = createSampleProfileItem();
        ColumnItem result2 = createSampleProfileItem();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    @Test
    public void testFromProtobuf() {
        Internal.Attr stringAttr = Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_STRING_TYPE)
                .setKey("string")
                .setStringValue("bar")
                .build();
        Internal.Attr uintAttr = Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_INT_TYPE)
                .setKey("int")
                .setIntValue(42L)
                .build();
        Internal.Attr boolAttr = Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_BOOL_TYPE)
                .setKey("bool")
                .setBoolValue(true)
                .build();
        Internal.Attr doubleAttr = Internal.Attr.newBuilder()
                .setType(Util.PROTOBUF_DOUBLE_TYPE)
                .setKey("double")
                .setFloatValue(3.14)
                .build();
        Internal.ColumnAttrSet column = Internal.ColumnAttrSet.newBuilder()
                .addAttrs(stringAttr)
                .addAttrs(uintAttr)
                .addAttrs(boolAttr)
                .addAttrs(doubleAttr)
                .setID(500L)
                .build();
        ColumnItem item = ColumnItem.fromInternal(column);
        Map<String, Object> attrs = item.getAttributes();
        assertEquals(500L, item.getID());
        assertEquals(4, attrs.size());
        assertEquals("bar", attrs.get("string"));
        assertEquals(42L, attrs.get("int"));
        assertEquals(true, attrs.get("bool"));
        assertEquals(3.14, attrs.get("double"));
    }

    private ColumnItem createSampleProfileItem() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("city", "Austin");
        return new ColumnItem(33, attrs);
    }
}
