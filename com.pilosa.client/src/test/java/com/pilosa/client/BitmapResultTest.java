package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class BitmapResultTest {
    @Test
    public void testCreateBitmapResult() {
        BitmapResult result = createSampleResult();
        Map<String, Object> attrs = result.getAttributes();
        assertEquals(1, attrs.size());
        assertEquals("blue", attrs.get("color"));
        List<Integer> bits = result.getBits();
        assertEquals(2, bits.size());
        assertEquals(42, (long) bits.get(0));
        assertEquals(45, (long) bits.get(1));
    }

    @Test
    public void testBitmapResultToString() {
        BitmapResult result = createSampleResult();
        String s = result.toString();
        assertEquals("BitmapResult(attrs={color=blue}, bits=[42, 45])", s);
    }

    private BitmapResult createSampleResult() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("color", "blue");
        List<Integer> bits = new ArrayList<>(2);
        bits.add(42);
        bits.add(45);
        return new BitmapResult(attrs, bits);
    }
}
