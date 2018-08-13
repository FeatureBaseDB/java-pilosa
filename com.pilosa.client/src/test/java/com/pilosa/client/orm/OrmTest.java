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

package com.pilosa.client.orm;

import com.pilosa.client.UnitTest;
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class OrmTest {
    private Index sampleIndex = Index.create("sample-db");
    private Field sampleField = sampleIndex.field("sample-field");
    private Index projectIndex;
    private Field collabField;

    {
        this.projectIndex = Index.create("project-db");
        FieldOptions collabFieldOptions = FieldOptions.withDefaults();
        this.collabField = projectIndex.field("collaboration", collabFieldOptions);
    }

    @Test
    public void pqlQueryCreate() {
        PqlBaseQuery pql = new PqlBaseQuery("Bitmap(field='foo', row=10)");
        assertEquals("Bitmap(field='foo', row=10)", pql.serialize());
        assertEquals(null, pql.getIndex());
    }

    @Test
    public void pqlRowQueryCreate() {
        PqlRowQuery pql = new PqlRowQuery("Bitmap(field='foo', row=10)");
        assertEquals("Bitmap(field='foo', row=10)", pql.serialize());
        assertEquals(null, pql.getIndex());
    }

    @Test
    public void batchTest() {
        PqlBatchQuery b = sampleIndex.batchQuery();
        b.add(sampleField.row(44));
        b.add(sampleField.row(10101));
        assertEquals(
                "Row(sample-field=44)Row(sample-field=10101)",
                b.serialize());
    }

    @Test
    public void batchWithCapacityTest() {
        PqlBatchQuery b = projectIndex.batchQuery(3);
        b.add(collabField.row(2));
        b.add(collabField.set(20, 40));
        b.add(collabField.topN(2));
        assertEquals(
                "Row(collaboration=2)" +
                        "Set(40,collaboration=20)" +
                        "TopN(collaboration,n=2)",
                b.serialize());
    }

    @Test
    public void nestedBatchTest() {
        PqlBatchQuery b1 = projectIndex.batchQuery(collabFrame.bitmap(2));
        PqlBatchQuery b2 = projectIndex.batchQuery(collabFrame.setBit(20, 40));
        PqlBatchQuery b = projectIndex.batchQuery(b1, b2, collabFrame.topN(2));
        assertEquals(
                "Bitmap(project=2, frame='collaboration')" +
                        "SetBit(project=20, frame='collaboration', user=40)" +
                        "TopN(frame='collaboration', n=2, inverse=false)",
                b.serialize());
    }

    @Test
    public void batchSizeTest() {
        PqlBatchQuery b = projectIndex.batchQuery();
        assertEquals(0, b.size());

        b.add(collabFrame.bitmap(2));
        assertEquals(1, b.size());
    }

    @Test(expected = PilosaException.class)
    public void batchAddFailsForDifferentDbsTest() {
        PqlBatchQuery b = projectIndex.batchQuery();
        b.add(sampleField.row(1));
    }

    @Test
    public void rowTest() {
        PqlBaseQuery qry1 = collabField.row(5);
        assertEquals(
                "Row(collaboration=5)",
                qry1.serialize());

        PqlBaseQuery qry2 = collabField.row("b7feb014-8ea7-49a8-9cd8-19709161ab63");
        assertEquals(
                "Row(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
                qry2.serialize());
    }

    @Test
    public void setTest() {
        PqlQuery qry = collabField.set(5, 10);
        assertEquals(
                "Set(10,collaboration=5)",
                qry.serialize());

        qry = collabField.set(5, "some_id");
        assertEquals(
                "Set('some_id',collaboration=5)",
                qry.serialize());

        qry = collabField.set("b7feb014-8ea7-49a8-9cd8-19709161ab63", 10);
        assertEquals(
                "Set(10,collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
                qry.serialize());

        qry = collabField.set("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id");
        assertEquals(
                "Set('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
                qry.serialize());
    }

    @Test
    public void setWithTimestampTest() {
        Calendar timestamp = Calendar.getInstance();
        timestamp.set(2017, Calendar.APRIL, 24, 12, 14);
        PqlQuery qry = collabField.set(10, 20, timestamp.getTime());
        assertEquals(
                "Set(20,collaboration=10,2017-04-24T12:14)",
                qry.serialize());

        qry = collabField.set(10, "mycol", timestamp.getTime());
        assertEquals(
                "Set('mycol',collaboration=10,2017-04-24T12:14)",
                qry.serialize());

        qry = collabField.set("myrow", 20, timestamp.getTime());
        assertEquals(
                "Set(20,collaboration='myrow',2017-04-24T12:14)",
                qry.serialize());

        qry = collabField.set("myrow", "mycol", timestamp.getTime());
        assertEquals(
                "Set('mycol',collaboration='myrow',2017-04-24T12:14)",
                qry.serialize());
    }

    @Test
    public void clearTest() {
        PqlQuery qry = collabField.clear(5, 10);
        assertEquals(
                "Clear(10,collaboration=5)",
                qry.serialize());

        qry = collabField.clear(5, "some_id");
        assertEquals(
                "Clear('some_id',collaboration=5)",
                qry.serialize());

        qry = collabField.clear("b7feb014-8ea7-49a8-9cd8-19709161ab63", 10);
        assertEquals(
                "Clear(10,collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
                qry.serialize());

        qry = collabField.clear("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id");
        assertEquals(
                "Clear('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
                qry.serialize());
    }

    @Test
    public void unionTest() {
        PqlRowQuery b1 = sampleField.row(10);
        PqlRowQuery b2 = sampleField.row(20);
        PqlRowQuery b3 = sampleField.row(42);
        PqlRowQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.union(b1, b2);
        assertEquals(
                "Union(Row(sample-field=10),Row(sample-field=20))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.union(b1, b2, b3);
        assertEquals(
                "Union(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.union(b1, b4);
        assertEquals(
                "Union(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize());
    }

    @Test
    public void union0Test() {
        PqlRowQuery q = sampleIndex.union();
        assertEquals("Union()", q.serialize());
    }

    @Test
    public void union1Test() {
        PqlRowQuery q = sampleIndex.union(sampleField.row(10));
        assertEquals("Union(Row(sample-field=10))", q.serialize());
    }

    @Test
    public void intersectTest() {
        PqlRowQuery b1 = sampleField.row(10);
        PqlRowQuery b2 = sampleField.row(20);
        PqlRowQuery b3 = sampleField.row(42);
        PqlRowQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.intersect(b1, b2);
        assertEquals(
                "Intersect(Row(sample-field=10),Row(sample-field=20))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.intersect(b1, b2, b3);
        assertEquals(
                "Intersect(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.intersect(b1, b4);
        assertEquals(
                "Intersect(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize());
    }

    @Test
    public void differenceTest() {
        PqlRowQuery b1 = sampleField.row(10);
        PqlRowQuery b2 = sampleField.row(20);
        PqlRowQuery b3 = sampleField.row(42);
        PqlRowQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.difference(b1, b2);
        assertEquals(
                "Difference(Row(sample-field=10),Row(sample-field=20))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.difference(b1, b2, b3);
        assertEquals(
                "Difference(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.difference(b1, b4);
        assertEquals(
                "Difference(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize());
    }

    @Test
    public void xorTest() {
        PqlRowQuery b1 = sampleField.row(10);
        PqlRowQuery b2 = sampleField.row(20);
        PqlBaseQuery q1 = sampleIndex.xor(b1, b2);
        assertEquals(
                "Xor(Row(sample-field=10),Row(sample-field=20))",
                q1.serialize());
    }

    @Test
    public void countTest() {
        PqlRowQuery b = collabField.row(42);
        PqlQuery q = projectIndex.count(b);
        assertEquals(
                "Count(Row(collaboration=42))",
                q.serialize());
    }

    @Test
    public void topNTest() {
        PqlQuery q1 = collabField.topN(27);
        assertEquals(
                "TopN(collaboration,n=27)",
                q1.serialize());

        PqlQuery q2 = collabField.topN(10, collabField.row(3));
        assertEquals(
                "TopN(collaboration,Row(collaboration=3),n=10)",
                q2.serialize());

        PqlBaseQuery q3 = sampleField.topN(12, collabField.row(7), "category", 80, 81);
        assertEquals(
                "TopN(sample-field,Row(collaboration=7),n=12,field='category',filters=[80,81])",
                q3.serialize());

        PqlBaseQuery q4 = sampleField.topN(12, null, "category", 80, 81);
        assertEquals(
                "TopN(sample-field,n=12,field='category',filters=[80,81])",
                q4.serialize());
    }

    @Test(expected = PilosaException.class)
    public void topNInvalidValuesTest() {
        sampleField.topN(5, sampleField.row(2), "category", 80, new Object());

    }

    @Test
    public void rangeTest() {
        Calendar start = Calendar.getInstance();
        start.set(1970, Calendar.JANUARY, 1, 0, 0);
        Calendar end = Calendar.getInstance();
        end.set(2000, Calendar.FEBRUARY, 2, 3, 4);
        PqlBaseQuery q = collabField.range(10, start.getTime(), end.getTime());
        assertEquals(
                "Range(collaboration=10,1970-01-01T00:00,2000-02-02T03:04)",
                q.serialize());
        q = collabField.range("foo", start.getTime(), end.getTime());
        assertEquals(
                "Range(collaboration='foo',1970-01-01T00:00,2000-02-02T03:04)",
                q.serialize());
    }

    @Test
    public void setRowAttrsTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("active", true);
        PqlQuery q = collabField.setRowAttrs(5, attrsMap);
        assertEquals(
                "SetRowAttrs(collaboration,5,active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
        q = collabField.setRowAttrs("foo", attrsMap);
        assertEquals(
                "SetRowAttrs('collaboration','foo',active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
    }

    @Test(expected = PilosaException.class)
    public void setRowAttrsInvalidValuesTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        collabField.setRowAttrs(5, attrsMap);
    }

    @Test
    public void setColumnAttrsTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("happy", true);
        PqlQuery q = projectIndex.setColumnAttrs(5, attrsMap);
        assertEquals(
                "SetColumnAttrs(5,happy=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
        q = projectIndex.setColumnAttrs("b7feb014-8ea7-49a8-9cd8-19709161ab63", attrsMap);
        assertEquals(
                "SetColumnAttrs('b7feb014-8ea7-49a8-9cd8-19709161ab63',happy=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
    }

    @Test(expected = PilosaException.class)
    public void setColumnAttrsInvalidValuesTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        projectIndex.setColumnAttrs(5, attrsMap);
    }

    @Test
    public void fieldLessThanTest() {
        PqlQuery q = collabField.lessThan(10);
        assertEquals(
                "Range(collaboration < 10)",
                q.serialize());
    }

    @Test
    public void fieldLessThanOrEqualTest() {
        PqlQuery q = collabField.lessThanOrEqual(10);
        assertEquals(
                "Range(collaboration <= 10)",
                q.serialize());

    }

    @Test
    public void fieldGreaterThanTest() {
        PqlQuery q = collabField.greaterThan(10);
        assertEquals(
                "Range(collaboration > 10)",
                q.serialize());

    }

    @Test
    public void fieldGreaterThanOrEqualTest() {
        PqlQuery q = collabField.greaterThanOrEqual(10);
        assertEquals(
                "Range(collaboration >= 10)",
                q.serialize());

    }

    @Test
    public void fieldEqualsTest() {
        PqlQuery q = collabField.equals(10);
        assertEquals(
                "Range(collaboration == 10)",
                q.serialize());
    }

    @Test
    public void fieldNotEqualsTest() {
        PqlQuery q = collabField.notEquals(10);
        assertEquals(
                "Range(collaboration != 10)",
                q.serialize());
    }

    @Test
    public void fieldNotNullTest() {
        PqlQuery q = collabField.notNull();
        assertEquals(
                "Range(collaboration != null)",
                q.serialize());
    }

    @Test
    public void fieldBetweenTest() {
        PqlQuery q = collabField.between(10, 20);
        assertEquals(
                "Range(collaboration >< [10,20])",
                q.serialize());

    }

    @Test
    public void fieldSumTest() {
        PqlQuery q = collabField.sum(collabField.row(10));
        assertEquals(
                "Sum(Row(collaboration=10),field='collaboration')",
                q.serialize());
        q = collabField.sum();
        assertEquals(
                "Sum(field='collaboration')",
                q.serialize()
        );
    }

    @Test
    public void fieldSetValueTest() {
        PqlQuery q = collabField.setValue(10, 20);
        assertEquals(
                "Set(10, collaboration=20)",
                q.serialize());
        q = collabField.setValue("foo", 100);
        assertEquals("Set('foo', collaboration=100)",
                q.serialize());
    }
}
