package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class CountResultItemTest {
    @Test
    public void testCreateCountResult() {
        CountResultItem result = createSampleResult();
        assertEquals(45, result.getKey());
        assertEquals(12, result.getCount());
    }

    @Test
    public void testCountResultToString() {
        CountResultItem result = createSampleResult();
        assertEquals("CountResultItem(key=45, count=12)", result.toString());
    }

    @Test
    public void testEquals() {
        CountResultItem result1 = createSampleResult();
        CountResultItem result2 = createSampleResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new CountResultItem()).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        CountResultItem result = createSampleResult();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        CountResultItem result1 = createSampleResult();
        CountResultItem result2 = createSampleResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    private CountResultItem createSampleResult() {
        return new CountResultItem(45, 12);
    }
}
