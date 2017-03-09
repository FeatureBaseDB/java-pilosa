package com.pilosa.client.orm;

import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.PilosaException;
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
                Pql.bitmap(5, "foo").toString());
    }

    @Test
    public void setBitTest() {
        assertEquals(
                "SetBit(id=2, frame=\"foo\", profileID=37)",
                Pql.setBit(2, "foo", 37).toString()
        );
    }

    @Test
    public void clearBitTest() {
        assertEquals(
                "ClearBit(id=2, frame=\"foo\", profileID=37)",
                Pql.clearBit(2, "foo", 37).toString()
        );
    }

    @Test
    public void unionTest() {
        assertEquals(
                "Union(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.union(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo")).toString()
        );
        assertEquals(
                "Union(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"bar\"), Bitmap(id=10, frame=\"zoo\"))",
                Pql.union(Pql.bitmap(2, "foo"),
                        Pql.bitmap(5, "bar"),
                        Pql.bitmap(10, "zoo")).toString()
        );
    }

    @Test
    public void intersectTest() {
        assertEquals(
                "Intersect(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.intersect(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo")).toString()
        );
        assertEquals(
                "Intersect(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"bar\"), Bitmap(id=10, frame=\"zoo\"))",
                Pql.intersect(Pql.bitmap(2, "foo"),
                        Pql.bitmap(5, "bar"),
                        Pql.bitmap(10, "zoo")).toString()
        );
    }

    @Test
    public void differenceTest() {
        assertEquals(
                "Difference(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"foo\"))",
                Pql.difference(Pql.bitmap(2, "foo"), Pql.bitmap(5, "foo")).toString()
        );
        assertEquals(
                "Difference(Bitmap(id=2, frame=\"foo\"), Bitmap(id=5, frame=\"bar\"), Bitmap(id=10, frame=\"zoo\"))",
                Pql.difference(Pql.bitmap(2, "foo"),
                        Pql.bitmap(5, "bar"),
                        Pql.bitmap(10, "zoo")).toString()
        );
    }

    @Test
    public void countTest() {
        assertEquals(
                "Count(Bitmap(id=2, frame=\"foo\"))",
                Pql.count(Pql.bitmap(2, "foo")).toString()
        );
    }

    @Test
    public void topNTest() {
        assertEquals(
                "TopN(frame=\"foo\", n=5)",
                Pql.topN("foo", 5).toString()
        );
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5)",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5).toString()
        );
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5, field=\"category\", [80,81])",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5, "category", 80, 81).toString()
        );
    }

    @Test(expected = PilosaException.class)
    public void topNInvalidValueTest() {
        assertEquals(
                "TopN(Bitmap(id=2, frame=\"foo\"), frame=\"foo\", n=5, field=\"category\", [80,81])",
                Pql.topN(Pql.bitmap(2, "foo"), "foo", 5, "category", 80, new Object()).toString()
        );
    }

    @Test
    public void rangeTest() {
        Calendar start = Calendar.getInstance();
        start.set(1970, 0, 1, 0, 0);
        Calendar end = Calendar.getInstance();
        end.set(2000, 1, 2, 3, 4);
        assertEquals(
                "Range(id=10, frame=\"foo\", start=\"1970-01-01T00:00\", end=\"2000-02-02T03:04\")",
                Pql.range(10, "foo", start.getTime(), end.getTime()).toString()
        );
    }

    @Test
    public void setBitmapAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", true);
        assertEquals(
                "SetBitmapAttrs(id=5, frame=\"foo\", color=\"blue\", happy=true)",
                Pql.setBitmapAttrs(5, "foo", attrsMap).toString()
        );
    }

    @Test(expected = PilosaException.class)
    public void setBitmapAttrsInvalidValueTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        assertEquals(
                "SetBitmapAttrs(id=5, frame=\"foo\", color=\"blue\", happy=true)",
                Pql.setBitmapAttrs(5, "foo", attrsMap).toString()
        );
    }

    @Test
    public void setProfileAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", true);
        assertEquals(
                "SetProfileAttrs(id=5, color=\"blue\", happy=true)",
                Pql.setProfileAttrs(5, attrsMap).toString()
        );
    }

    @Test(expected = PilosaException.class)
    public void setProfileAttrsInvalidValueTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        assertEquals(
                "SetProfileAttrs(id=5, color=\"blue\", happy=true)",
                Pql.setProfileAttrs(5, attrsMap).toString()
        );
    }

    @Test
    public void createPqlTest() {
        // this test is required only to get 100% coverage
        new Pql();
    }
}
