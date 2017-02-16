package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class ValidatorTest {
    @Test
    public void validDatabaseNameTest() {
        assertValidDatabaseNames(new String[]{
                "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });
    }

    @Test
    public void validFrameNameTest() {
        assertValidFrameNames(new String[]{
                "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });
    }

    @Test
    public void invalidDatabaseNameTest() {
        assertInvalidDatabaseNames(new String[]{
                "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        });
    }

    @Test
    public void invalidFrameNameTest() {
        assertInvalidFrameNames(new String[]{
                "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        });
    }

    private void assertValidDatabaseNames(String []names) {
        for (String name : names) {
            assertTrue(Validator.validateDatabaseName(name));
        }
    }

    private void assertInvalidDatabaseNames(String []names) {
        for (String name : names) {
            assertFalse(Validator.validateDatabaseName(name));
        }
    }

    private void assertValidFrameNames(String []names) {
        for (String name : names) {
            assertTrue(Validator.validateFrameName(name));
        }
    }

    private void assertInvalidFrameNames(String []names) {
        for (String name : names) {
            assertFalse(Validator.validateFrameName(name));
        }
    }

    @Test
    public void createValidatorTest() {
        // this test is required only to get 100% coverage
        new Validator();
    }
}
