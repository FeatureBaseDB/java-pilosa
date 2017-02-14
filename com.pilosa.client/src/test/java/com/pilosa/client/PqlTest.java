package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class PqlTest {
    @Test
    public void bitmapTest() {
        assertEquals(
                "Bitmap(id=5, frame=\"foo\")",
                Pql.bitmap(5, "foo"));
    }

    @Test
    public void setBitTest() {
        assertEquals(
                "SetBit(id=2, frame=\"foo\", profileID=37)",
                Pql.setBit(2, "foo", 37)
        );
    }

    @Test
    public void clearBitTest() {
        assertEquals(
                "ClearBit(id=2, frame=\"foo\", profileID=37)",
                Pql.clearBit(2, "foo", 37)
        );
    }

    @Test
    public void unionTest() {
        assertEquals(
                "Union(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.union(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo"))
        );
    }

    @Test
    public void intersectTest() {
        assertEquals(
                "Intersect(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.intersect(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo"))
        );
    }

    @Test
    public void differenceTest() {
        assertEquals(
                "Difference(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.difference(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo"))
        );
    }

    @Test
    public void countTest() {
        assertEquals(
                "Count(Bitmap(id=2, frame=\"foo\"))",
                Pql.count(Pql.bitmap(2, "foo"))
        );
    }

    @Test
    public void topNTest() {
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5)",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5)
        );
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5, field=\"category\", [80,81])",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5, "category", 80, 81)
        );
    }

    @Test(expected = PilosaException.class)
    public void topNInvalidValueTest() {
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5, field=\"category\", [80,81])",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5, "category", 80, new Object())
        );
    }

    @Test
    public void createPqlTest() {
        // this test is required only to get 100% coverage
        new Pql();
    }

    @Test
    public void rangeTest() {
        Calendar start = Calendar.getInstance();
        start.set(1970, 0, 1, 0, 0);
        Calendar end = Calendar.getInstance();
        end.set(2000, 1, 2, 3, 4);
        assertEquals(
                "Range(id=10, frame=\"foo\", start=\"1970-01-01T00:00\", end=\"2000-02-02T03:04\")",
                Pql.range(10, "foo", start.getTime(), end.getTime())
        );
    }

    @Test
    public void setBitmapAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", true);
        assertEquals(
                "SetBitmapAttrs(id=5, frame=\"foo\", color=\"blue\", happy=true)",
                Pql.setBitmapAttrs(5, "foo", attrsMap)
        );
    }

    @Test(expected = PilosaException.class)
    public void setBitmapAttrsInvalidValueTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        assertEquals(
                "SetBitmapAttrs(id=5, frame=\"foo\", color=\"blue\", happy=true)",
                Pql.setBitmapAttrs(5, "foo", attrsMap)
        );
    }

    @Test
    public void setProfileAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", true);
        assertEquals(
                "SetProfileAttrs(id=5, color=\"blue\", happy=true)",
                Pql.setProfileAttrs(5, attrsMap)
        );
    }

    @Test(expected = PilosaException.class)
    public void setProfileAttrsInvalidValueTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        assertEquals(
                "SetProfileAttrs(id=5, color=\"blue\", happy=true)",
                Pql.setProfileAttrs(5, attrsMap)
        );
    }
}
