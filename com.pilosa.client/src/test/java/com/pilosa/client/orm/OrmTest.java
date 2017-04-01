package com.pilosa.client.orm;

import com.pilosa.client.DatabaseOptions;
import com.pilosa.client.FrameOptions;
import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class OrmTest {
    private Database sampleDb = Database.named("sample-db");
    private Frame sampleFrame = sampleDb.frame("sample-frame");
    private Database projectDb;
    private Frame collabFrame;

    {
        DatabaseOptions projectDbOptions = new DatabaseOptions.Builder()
                .setColumnLabel("user")
                .build();
        this.projectDb = Database.named("project-db", projectDbOptions);
        FrameOptions collabFrameOptions = new FrameOptions.Builder()
                .setRowLabel("project")
                .build();
        this.collabFrame = projectDb.frame("collaboration", collabFrameOptions);
    }

    @Test
    public void pqlQueryCreate() {
        PqlBaseQuery pql = new PqlBaseQuery("Bitmap(frame='foo', id=10)");
        assertEquals("Bitmap(frame='foo', id=10)", pql.toString());
        assertEquals(null, pql.getDatabase());
    }

    @Test
    public void pqlBitmapQueryCreate() {
        PqlBitmapQuery pql = new PqlBitmapQuery("Bitmap(frame='foo', id=10)");
        assertEquals("Bitmap(frame='foo', id=10)", pql.toString());
        assertEquals(null, pql.getDatabase());

    }

    @Test
    public void batchTest() {
        BatchQuery b = sampleDb.batchQuery();
        b.add(sampleFrame.bitmap(44));
        b.add(sampleFrame.bitmap(10101));
        assertEquals(
                "Bitmap(id=44, frame='sample-frame')Bitmap(id=10101, frame='sample-frame')",
                b.toString());
    }

    @Test
    public void batchWithCapacityTest() {
        BatchQuery b = projectDb.batchQuery(3);
        b.add(collabFrame.bitmap(2));
        b.add(collabFrame.setBit(20, 40));
        b.add(collabFrame.topN(2));
        assertEquals(
                "Bitmap(project=2, frame='collaboration')" +
                        "SetBit(project=20, frame='collaboration', user=40)" +
                        "TopN(frame='collaboration', n=2)",
                b.toString());
    }

    @Test(expected = PilosaException.class)
    public void batchAddFailsForDifferentDbsTest() {
        BatchQuery b = projectDb.batchQuery();
        b.add(sampleFrame.bitmap(1));
    }

    @Test
    public void bitmapTest() {
        PqlBaseQuery qry1 = sampleFrame.bitmap(5);
        assertEquals(
                "Bitmap(id=5, frame='sample-frame')",
                qry1.toString());

        PqlBaseQuery qry2 = collabFrame.bitmap(10);
        assertEquals(
                "Bitmap(project=10, frame='collaboration')",
                qry2.toString());
    }

    @Test
    public void setBitTest() {
        PqlBaseQuery qry1 = sampleFrame.setBit(5, 10);
        assertEquals(
                "SetBit(id=5, frame='sample-frame', col_id=10)",
                qry1.toString());

        PqlBaseQuery qry2 = collabFrame.setBit(10, 20);
        assertEquals(
                "SetBit(project=10, frame='collaboration', user=20)",
                qry2.toString());
    }

    @Test
    public void clearBitTest() {
        PqlBaseQuery qry1 = sampleFrame.clearBit(5, 10);
        assertEquals(
                "ClearBit(id=5, frame='sample-frame', col_id=10)",
                qry1.toString());

        PqlBaseQuery qry2 = collabFrame.clearBit(10, 20);
        assertEquals(
                "ClearBit(project=10, frame='collaboration', user=20)",
                qry2.toString());
    }

    @Test
    public void unionTest() {
        PqlBitmapQuery b1 = sampleFrame.bitmap(10);
        PqlBitmapQuery b2 = sampleFrame.bitmap(20);
        PqlBitmapQuery b3 = sampleFrame.bitmap(42);
        PqlBitmapQuery b4 = collabFrame.bitmap(2);

        PqlBaseQuery q1 = sampleDb.union(b1, b2);
        assertEquals(
                "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
                q1.toString());

        PqlBaseQuery q2 = sampleDb.union(b1, b2, b3);
        assertEquals(
                "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
                q2.toString());

        PqlBaseQuery q3 = sampleDb.union(b1, b4);
        assertEquals(
                "Union(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
                q3.toString());
    }

    @Test
    public void intersectTest() {
        PqlBitmapQuery b1 = sampleFrame.bitmap(10);
        PqlBitmapQuery b2 = sampleFrame.bitmap(20);
        PqlBitmapQuery b3 = sampleFrame.bitmap(42);
        PqlBitmapQuery b4 = collabFrame.bitmap(2);

        PqlBaseQuery q1 = sampleDb.intersect(b1, b2);
        assertEquals(
                "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
                q1.toString());

        PqlBaseQuery q2 = sampleDb.intersect(b1, b2, b3);
        assertEquals(
                "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
                q2.toString());

        PqlBaseQuery q3 = sampleDb.intersect(b1, b4);
        assertEquals(
                "Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
                q3.toString());
    }

    @Test
    public void differenceTest() {
        PqlBitmapQuery b1 = sampleFrame.bitmap(10);
        PqlBitmapQuery b2 = sampleFrame.bitmap(20);
        PqlBitmapQuery b3 = sampleFrame.bitmap(42);
        PqlBitmapQuery b4 = collabFrame.bitmap(2);

        PqlBaseQuery q1 = sampleDb.difference(b1, b2);
        assertEquals(
                "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
                q1.toString());

        PqlBaseQuery q2 = sampleDb.difference(b1, b2, b3);
        assertEquals(
                "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
                q2.toString());

        PqlBaseQuery q3 = sampleDb.difference(b1, b4);
        assertEquals(
                "Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
                q3.toString());
    }

    @Test
    public void countTest() {
        PqlBitmapQuery b = collabFrame.bitmap(42);
        PqlBaseQuery q = projectDb.count(b);
        assertEquals(
                "Count(Bitmap(project=42, frame='collaboration'))",
                q.toString());
    }

    @Test
    public void topNTest() {
        PqlBaseQuery q1 = sampleFrame.topN(27);
        assertEquals(
                "TopN(frame='sample-frame', n=27)",
                q1.toString());

        PqlBaseQuery q2 = sampleFrame.topN(10, collabFrame.bitmap(3));
        assertEquals(
                "TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10)",
                q2.toString());

        PqlBaseQuery q3 = sampleFrame.topN(12, collabFrame.bitmap(7), "category", 80, 81);

        assertEquals(
                "TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, field='category', [80,81])",
                q3.toString());
    }

    @Test(expected = PilosaException.class)
    public void topNInvalidValuesTest() {
        sampleFrame.topN(5, sampleFrame.bitmap(2), "category", 80, new Object());

    }

    @Test
    public void rangeTest() {
        Calendar start = Calendar.getInstance();
        start.set(1970, Calendar.JANUARY, 1, 0, 0);
        Calendar end = Calendar.getInstance();
        end.set(2000, Calendar.FEBRUARY, 2, 3, 4);
        PqlBaseQuery q = collabFrame.range(10, start.getTime(), end.getTime());
        assertEquals(
                "Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
                q.toString());


    }

    @Test
    public void setBitmapAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("active", true);
        PqlBaseQuery q = collabFrame.setBitmapAttrs(5, attrsMap);
        assertEquals(
                "SetBitmapAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
                q.toString());
    }

    @Test(expected = PilosaException.class)
    public void setBitmapAttrsInvalidValuesTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        collabFrame.setBitmapAttrs(5, attrsMap);
    }

    @Test
    public void setProfileAttrsTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("happy", true);
        PqlBaseQuery q = projectDb.setProfileAttrs(5, attrsMap);
        assertEquals(
                "SetProfileAttrs(user=5, quote=\"\\\"Don't worry, be happy\\\"\", happy=true)",
                q.toString());
    }

    @Test(expected = PilosaException.class)
    public void setProfileAttrsInvalidValuesTest() {
        Map<String, Object> attrsMap = new HashMap<>(2);
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        projectDb.setProfileAttrs(5, attrsMap);
    }
}
