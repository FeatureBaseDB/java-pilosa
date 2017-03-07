package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class BitmapResultTest {
    @Test
    public void testCreateBitmapResult() {
        BitmapResult result = createSampleResult();
        Map<String, Object> attrs = result.getAttributes();
        assertEquals(1, attrs.size());
        assertEquals("blue", attrs.get("color"));
        List<Long> bits = result.getBits();
        assertEquals(2, bits.size());
        assertEquals(42, (long) bits.get(0));
        assertEquals(45, (long) bits.get(1));
    }

    @Test
    public void testCreateBitmapResultDefaultConstructor() {
        new BitmapResult();
    }

    @Test
    public void testBitmapResultToString() {
        BitmapResult result = createSampleResult();
        String s = result.toString();
        assertEquals("BitmapResult(attrs={color=blue}, bits=[42, 45])", s);
    }

    @Test
    public void testEquals() {
        BitmapResult result1 = createSampleResult();
        BitmapResult result2 = createSampleResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new BitmapResult(null, null)).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        BitmapResult result = createSampleResult();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        BitmapResult result1 = createSampleResult();
        BitmapResult result2 = createSampleResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    private BitmapResult createSampleResult() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("color", "blue");
        List<Long> bits = new ArrayList<>(2);
        bits.add(42L);
        bits.add(45L);
        return new BitmapResult(attrs, bits);
    }
}
