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
    public void createProfileItem() {
        ProfileItem pi = createSampleProfileItem();
        assertEquals(33, pi.getID());
        assertEquals("Austin", pi.getAttributes().get("city"));
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
        boolean e = (new ProfileItem()).equals(0);
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

    @Test(expected = PilosaException.class)
    public void testFromMapNoId() {
        Map<String, Object> m = new HashMap<>(1);
        m.put("attrs", new HashMap<>(0));
        ProfileItem.fromMap(m);
    }

    @Test(expected = PilosaException.class)
    public void testFromMapNoAttrs() {
        Map<String, Object> m = new HashMap<>(0);
        m.put("id", 44);
        ProfileItem.fromMap(m);
    }

    @Test
    public void testFromMap() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("city", "Austin");
        Map<String, Object> m = new HashMap<>(0);
        m.put("id", 33);
        m.put("attrs", attrs);
        assertEquals(createSampleProfileItem(), ProfileItem.fromMap(m));
    }

    private ProfileItem createSampleProfileItem() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("city", "Austin");
        return new ProfileItem(33, attrs);
    }
}
