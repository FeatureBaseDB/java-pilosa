package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class PilosURIExceptionTest {
    @Test
    public void createPilosaURIException() {
        try {
            throw new PilosaURIException();
        } catch (PilosaURIException ex) {
            assertEquals(null, ex.getMessage());
        }

        try {
            throw new PilosaURIException("malformed URI");
        } catch (PilosaURIException ex) {
            assertEquals("malformed URI", ex.getMessage());
        }
    }
}
