package com.pilosa.client;

import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class TimeQuantumTest {
    @Test
    public void testGetStringValue() {
        Map<TimeQuantum, String> vs = getTargetMap();
        for (Map.Entry<TimeQuantum, String> e : vs.entrySet()) {
            assertEquals(e.getValue(), e.getKey().getStringValue());
        }
    }

    @Test
    public void testFromString() {
        Map<TimeQuantum, String> vs = getTargetMap();
        for (Map.Entry<TimeQuantum, String> e : vs.entrySet()) {
            assertEquals(TimeQuantum.fromString(e.getValue()), e.getKey());
        }
    }

    @Test(expected = ValidationException.class)
    public void testFromStringInvalidString() {
        TimeQuantum.fromString("INV");
    }

    private Map<TimeQuantum, String> getTargetMap() {
        Map<TimeQuantum, String> vs = new HashMap<>();
        vs.put(TimeQuantum.YEAR_MONTH_DAY_HOUR, "YMDH");
        vs.put(TimeQuantum.YEAR_MONTH_DAY, "YMD");
        vs.put(TimeQuantum.YEAR_MONTH, "YM");
        vs.put(TimeQuantum.YEAR, "Y");
        vs.put(TimeQuantum.MONTH_DAY_HOUR, "MDH");
        vs.put(TimeQuantum.MONTH_DAY, "MD");
        vs.put(TimeQuantum.MONTH, "M");
        vs.put(TimeQuantum.DAY, "D");
        vs.put(TimeQuantum.DAY_HOUR, "DH");
        vs.put(TimeQuantum.HOUR, "H");
        vs.put(TimeQuantum.NONE, "");
        return vs;
    }
}
