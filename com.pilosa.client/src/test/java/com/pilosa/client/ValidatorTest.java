package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(UnitTest.class)
public class ValidatorTest {
    private final static String[] validIndexNames = new String[]{
            "a", "ab", "ab1", "1", "_", "-", "b-c", "d_e",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    private final static String[] invalidIndexNames = new String[]{
            "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    };
    private final static String[] validFrameNames = new String[]{
            "a", "ab", "ab1", "b-c", "d_e", "d.e", "1",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    private final static String[] invalidFrameNames = new String[]{
            "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "_", "-", ".data",
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
    public void validIndexNameTest() {
        for (String name : validIndexNames) {
            assertTrue(Validator.validIndexName(name));
        }
    }

    @Test
    public void invalidIndexNameTest() {
        for (String name : invalidIndexNames) {
            assertFalse(Validator.validIndexName(name));
        }
    }

    @Test
    public void ensureValidIndexNameTest() {
        for (String name : validIndexNames) {
            Validator.ensureValidIndexName(name);
        }
    }

    @Test
    public void ensureValidIndexNameFailsTest() {
        for (String name : invalidIndexNames) {
            try {
                Validator.ensureValidIndexName(name);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Validation should have failed for: " + name);
        }
    }

    @Test
    public void invalidFrameNameTest() {
        for (String name : invalidFrameNames) {
            assertFalse(Validator.validFrameName(name));
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
            assertTrue(Validator.validLabel(label));
        }
    }

    @Test
    public void invalidLabelTest() {
        for (String label : invalidLabels) {
            assertFalse(Validator.validLabel(label));
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
