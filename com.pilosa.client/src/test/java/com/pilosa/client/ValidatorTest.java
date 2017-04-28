/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

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
            } catch (ValidationException ex) {
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
