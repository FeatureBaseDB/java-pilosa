package com.pilosa.client.orm;

import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class IndexTest {
    @Test(expected = ValidationException.class)
    public void checkValidatorWasCalledTest() {
        Index.withName("a:b");
    }

    @Test
    public void checkUnionArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("union", 0));
        assertEquals(true, checkArguments("union", 1));
        assertEquals(false, checkArguments("union", 2));
    }

    @Test
    public void checkIntersectArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("intersect", 0));
        assertEquals(true, checkArguments("intersect", 1));
        assertEquals(false, checkArguments("intersect", 2));
    }

    @Test
    public void checkDifferenceArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("difference", 0));
        assertEquals(true, checkArguments("difference", 1));
        assertEquals(false, checkArguments("difference", 2));
    }

    private boolean checkArguments(String methodName, int count)
            throws NoSuchMethodException, IllegalAccessException {
        Index index = Index.withName("my-index");
        Frame frame = index.frame("my-frame");
        Method m = index.getClass().getMethod(methodName, PqlBitmapQuery[].class);
        PqlBitmapQuery queries[] = new PqlBitmapQuery[count];
        for (int i = 0; i < count; i++) {
            queries[i] = frame.bitmap(i);
        }
        try {
            m.invoke(index, (Object) queries);
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause == null) {
                return false;
            }
            if (cause.getClass().equals(IllegalArgumentException.class)) {
                return true;
            }
        }
        return false;
    }
}