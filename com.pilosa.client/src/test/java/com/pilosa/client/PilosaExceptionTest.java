package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class PilosaExceptionTest {
    @Test
    public void createPilosaException() {
        try {
            throw new PilosaException();
        } catch (PilosaException ex) {
            assertEquals(null, ex.getMessage());
        }

        try {
            throw new PilosaException("no exception");
        } catch (PilosaException ex) {
            assertEquals("no exception", ex.getMessage());
        }
    }
}
