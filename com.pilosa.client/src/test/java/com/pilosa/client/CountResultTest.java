package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class CountResultTest {
    @Test
    public void testCreateCountResult() {
        CountResult result = createSampleResult();
        assertEquals(45, result.getKey());
        assertEquals(12, result.getCount());
    }

    @Test
    public void testCountResultToString() {
        CountResult result = createSampleResult();
        assertEquals("CountResult(key=45, count=12)", result.toString());
    }

    private CountResult createSampleResult() {
        return new CountResult(45, 12);
    }
}
