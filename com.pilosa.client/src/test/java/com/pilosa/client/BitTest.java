package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class BitTest {
    @Test
    public void hashCodeTest() {
        Bit bit1 = Bit.create(1, 10, 65000);
        Bit bit2 = Bit.create(1, 10, 65000);
        assertEquals(bit1.hashCode(), bit2.hashCode());
    }

    @Test
    public void equalsSameObjectTest() {
        Bit bit = Bit.create(5, 7, 100000);
        assertTrue(bit.equals(bit));
    }

    @Test
    public void notEqualTest() {
        Bit bit = Bit.create(15, 2, 50000);
        assertFalse(bit.equals(5));
    }
}
