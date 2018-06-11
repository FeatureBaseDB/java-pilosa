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

import static org.junit.Assert.fail;

@Category(UnitTest.class)
public class ValidatorTest {
    @Test
    public void ensureValidIndexNameTest() {
        String[] validIndexNames = new String[]{
                "a", "ab", "ab1", "b-c", "d_e",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        };

        for (String name : validIndexNames) {
            Validator.ensureValidIndexName(name);
        }
    }

    @Test
    public void ensureValidIndexNameFailsTest() {
        String[] invalidIndexNames = new String[]{
                "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1", "1", "_", "-",
        };
        for (String name : invalidIndexNames) {
            try {
                Validator.ensureValidIndexName(name);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Index name validation should have failed for: " + name);
        }
    }

    @Test
    public void ensureValidFrameNameTest() {
        String[] validFrameNames = new String[]{
                "a", "ab", "ab1", "b-c", "d_e",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        };
        for (String name : validFrameNames) {
            Validator.ensureValidFrameName(name);
        }
    }

    @Test
    public void ensureValidFrameNameFailsTest() {
        String[] invalidFrameNames = new String[]{
                "", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "_", "-", ".data", "d.e", "1",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        };
        for (String name : invalidFrameNames) {
            try {
                Validator.ensureValidFrameName(name);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Field name validation should have failed for: " + name);
        }
    }

    @Test
    public void ensureValidLabelTest() {
        String[] validLabels = new String[]{
                "a", "ab", "ab1", "d_e", "A", "Bc", "B1", "aB", "b-c",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        };
        for (String label : validLabels) {
            Validator.ensureValidLabel(label);
        }
    }

    @Test
    public void ensureValidLabelFailsTest() {
        String[] invalidLabels = new String[]{
                "", "1", "_", "-", "'", "^", "/", "\\", "*", "a:b", "valid?no", "yüce",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        };
        for (String label : invalidLabels) {
            try {
                Validator.ensureValidLabel(label);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Label validation should have failed for: " + label);
        }
    }

    @Test
    public void ensureValidKeyTest() {
        String[] validKeys = new String[]{
                "", "1", "ab", "ab1", "b-c", "d_e", "pilosa.com",
                "bbf8d41c-7dba-40c4-94dc-94677b43bcf3",  // UUID
                "{bbf8d41c-7dba-40c4-94dc-94677b43bcf3}",  // Windows GUID
                "https%3A//www.pilosa.com/about/%23contact",  // escaped URL
                "aHR0cHM6Ly93d3cucGlsb3NhLmNvbS9hYm91dC8jY29udGFjdA==",  // base64
                "urn:isbn:1234567",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        };
        for (String key : validKeys) {
            Validator.ensureValidKey(key);
        }
    }

    @Test
    public void ensureValidKeyFailsTest() {
        String[] invalidKeys = new String[]{
                "\"", "'", "slice\\dice", "valid?no", "yüce", "*xyz", "with space", "<script>",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
        };
        for (String key : invalidKeys) {
            try {
                Validator.ensureValidKey(key);
            } catch (ValidationException ex) {
                continue;
            }
            fail("Key validation should have failed for: " + key);
        }
    }

    @Test
    public void createValidatorTest() {
        // this test is required only to get 100% coverage
        new Validator();
    }
}
