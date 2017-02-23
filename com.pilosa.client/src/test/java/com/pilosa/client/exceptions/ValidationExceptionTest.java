package com.pilosa.client.exceptions;

import com.pilosa.client.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ValidationExceptionTest {
    @Test
    public void createValidationExceptionTest() {
        try {
            throw new ValidationException();
        } catch (ValidationException ex) {
            assertEquals(null, ex.getMessage());
        }

        try {
            throw new ValidationException("invalid thing");
        } catch (ValidationException ex) {
            assertEquals("invalid thing", ex.getMessage());
        }
    }
}
