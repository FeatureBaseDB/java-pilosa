package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class BitTest {
    @Test
    public void hashCodeTest() {
        Bit bit1 = Bit.create(1, 10, 65000);
        Bit bit2 = Bit.create(1, 10, 65000);
        assertEquals(bit1.hashCode(), bit2.hashCode());
    }
}
