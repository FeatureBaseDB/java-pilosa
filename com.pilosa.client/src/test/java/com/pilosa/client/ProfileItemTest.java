package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class ProfileItemTest {
    @Test
    public void testCreateProfileItem() {
        ProfileItem pi = createSampleProfileItem();
        assertEquals(33, pi.getID());
        assertEquals("Austin", pi.getAttributes().get("city"));
    }

    @Test
    public void testCreateProfileItemDefaultConstructor() {
        new ProfileItem();
    }

    @Test
    public void testBitmapResultToString() {
        ProfileItem result = createSampleProfileItem();
        String s = result.toString();
        assertEquals("ProfileItem(id=33, attrs={city=Austin})", s);
    }

    @Test
    public void testEquals() {
        ProfileItem result1 = createSampleProfileItem();
        ProfileItem result2 = createSampleProfileItem();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new ProfileItem(1, null)).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        ProfileItem result = createSampleProfileItem();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        ProfileItem result1 = createSampleProfileItem();
        ProfileItem result2 = createSampleProfileItem();
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
                .setType(Util.PROTOBUF_UINT_TYPE)
                .setKey("uint")
                .setUintValue(42L)
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
        Internal.Profile profile = Internal.Profile.newBuilder()
                .addAttrs(stringAttr)
                .addAttrs(uintAttr)
                .addAttrs(boolAttr)
                .addAttrs(doubleAttr)
                .setID(500L)
                .build();
        ProfileItem item = ProfileItem.fromInternal(profile);
        Map<String, Object> attrs = item.getAttributes();
        assertEquals(500L, item.getID());
        assertEquals(4, attrs.size());
        assertEquals("bar", attrs.get("string"));
        assertEquals(42L, attrs.get("uint"));
        assertEquals(true, attrs.get("bool"));
        assertEquals(3.14, attrs.get("double"));
    }

    private ProfileItem createSampleProfileItem() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("city", "Austin");
        return new ProfileItem(33, attrs);
    }
}
