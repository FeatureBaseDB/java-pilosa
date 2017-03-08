package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(UnitTest.class)
public class ValidatorTest {
    private final static String[] validDatabaseNames = new String[]{
            "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    private final static String[] invalidDatabaseNames = new String[]{
            "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    };
    private final static String[] validFrameNames = new String[]{
            "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e", "d.e",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    private final static String[] invalidFrameNames = new String[]{
            "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    };
    private final static String[] validLabels = new String[]{
            "a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    private final static String[] invalidLabels = new String[]{
            "", "1", "_", "-", "b-c", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    };

    @Test
    public void validDatabaseNameTest() {
        for (String name : validDatabaseNames) {
            assertTrue(Validator.validateDatabaseName(name));
        }
    }

    @Test
    public void invalidDatabaseNameTest() {
        for (String name : invalidDatabaseNames) {
            assertFalse(Validator.validateDatabaseName(name));
        }
    }

    @Test
    public void ensureValidDatabaseNameTest() {
        for (String name : validDatabaseNames) {
            Validator.ensureValidDatabaseName(name);
        }
    }

    @Test
    public void ensureValidDatabaseNameFailsTest() {
        for (String name : invalidDatabaseNames) {
            try {
                Validator.ensureValidDatabaseName(name);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Validation should have failed for: " + name);
        }
    }

    @Test
    public void invalidFrameNameTest() {
        for (String name : invalidFrameNames) {
            assertFalse(Validator.validateFrameName(name));
        }
    }

    @Test
    public void ensureValidFrameNameTest() {
        for (String name : validFrameNames) {
            Validator.ensureValidFrameName(name);
        }
    }

    @Test
    public void ensureValidFrameNameFailsTest() {
        for (String name : invalidFrameNames) {
            try {
                Validator.ensureValidFrameName(name);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Validation should have failed for: " + name);
        }
    }

    @Test
    public void validLabelTest() {
        for (String label : validLabels) {
            assertTrue(Validator.validateLabel(label));
        }
    }

    @Test
    public void invalidLabelTest() {
        for (String label : invalidLabels) {
            assertFalse(Validator.validateLabel(label));
        }
    }

    @Test
    public void ensureValidLabelTest() {
        for (String label : validLabels) {
            Validator.ensureValidLabel(label);
        }
    }

    @Test
    public void ensureValidLabelFailsTest() {
        for (String label : invalidLabels) {
            try {
                Validator.ensureValidLabel(label);
            }
            catch (ValidationException ex) {
                continue;
            }
            fail("Validation should have failed for: " + label);
        }
    }

    @Test
    public void createValidatorTest() {
        // this test is required only to get 100% coverage
        new Validator();
    }
}
