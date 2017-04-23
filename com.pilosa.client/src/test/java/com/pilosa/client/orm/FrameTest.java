package com.pilosa.client.orm;

import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FrameTest {
    @Test(expected = ValidationException.class)
    public void checkValidatorWasCalledTest() {
        Frame frame = Index.withName("foo").frame("a:b");
    }
}
