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
    private Index sampleIndex = Index.withName("sample-db");
    private Field sampleField = sampleIndex.field("sample-field");
    private Index projectIndex;
    private Field collabField;

    {
        this.projectIndex = Index.withName("project-db");
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
    public void pqlBitmapQueryCreate() {
        PqlBitmapQuery pql = new PqlBitmapQuery("Bitmap(field='foo', row=10)");
        assertEquals("Bitmap(field='foo', row=10)", pql.serialize());
        assertEquals(null, pql.getIndex());
    }

    @Test
    public void batchTest() {
        PqlBatchQuery b = sampleIndex.batchQuery();
        b.add(sampleField.row(44));
        b.add(sampleField.row(10101));
        assertEquals(
                "Bitmap(row=44, field='sample-field')Bitmap(row=10101, field='sample-field')",
                b.serialize());
    }

    @Test
    public void batchWithCapacityTest() {
        PqlBatchQuery b = projectIndex.batchQuery(3);
        b.add(collabField.row(2));
        b.add(collabField.setBit(20, 40));
        b.add(collabField.topN(2));
        assertEquals(
                "Bitmap(row=2, field='collaboration')" +
                        "SetBit(row=20, field='collaboration', col=40)" +
                        "TopN(field='collaboration', n=2)",
                b.serialize());
    }

    @Test(expected = PilosaException.class)
    public void batchAddFailsForDifferentDbsTest() {
        PqlBatchQuery b = projectIndex.batchQuery();
        b.add(sampleField.row(1));
    }

    @Test
    public void bitmapTest() {
        PqlBaseQuery qry1 = sampleField.row(5);
        assertEquals(
                "Bitmap(row=5, field='sample-field')",
                qry1.serialize());

        PqlBaseQuery qry2 = collabField.row(10);
        assertEquals(
                "Bitmap(row=10, field='collaboration')",
                qry2.serialize());
        PqlBaseQuery qry3 = sampleField.row("b7feb014-8ea7-49a8-9cd8-19709161ab63");
        assertEquals(
                "Bitmap(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='sample-field')",
                qry3.serialize());
    }

    @Test
    public void setBitTest() {
        PqlQuery qry1 = sampleField.setBit(5, 10);
        assertEquals(
                "SetBit(row=5, field='sample-field', col=10)",
                qry1.serialize());

        PqlQuery qry2 = collabField.setBit(10, 20);
        assertEquals(
                "SetBit(row=10, field='collaboration', col=20)",
                qry2.serialize());
        PqlQuery qry3 = sampleField.setBit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id");
        assertEquals(
                "SetBit(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='sample-field', col='some_id')",
                qry3.serialize());

    }

    @Test
    public void setBitWithTimestampTest() {
        Calendar timestamp = Calendar.getInstance();
        timestamp.set(2017, Calendar.APRIL, 24, 12, 14);
        PqlQuery qry = collabField.setBit(10, 20, timestamp.getTime());
        assertEquals(
                "SetBit(row=10, field='collaboration', col=20, timestamp='2017-04-24T12:14')",
                qry.serialize());
        PqlQuery qry2 = collabField.setBit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some", timestamp.getTime());
        assertEquals(
                "SetBit(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='collaboration', col='some', timestamp='2017-04-24T12:14')",
                qry2.serialize());
        PqlQuery qry3 = sampleField.clearBit("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id");
        assertEquals(
                "ClearBit(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='sample-field', col='some_id')",
                qry3.serialize());

    }

    @Test
    public void clearBitTest() {
        PqlQuery qry1 = sampleField.clearBit(5, 10);
        assertEquals(
                "ClearBit(row=5, field='sample-field', col=10)",
                qry1.serialize());

        PqlQuery qry2 = collabField.clearBit(10, 20);
        assertEquals(
                "ClearBit(row=10, field='collaboration', col=20)",
                qry2.serialize());
    }

    @Test
    public void unionTest() {
        PqlBitmapQuery b1 = sampleField.row(10);
        PqlBitmapQuery b2 = sampleField.row(20);
        PqlBitmapQuery b3 = sampleField.row(42);
        PqlBitmapQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.union(b1, b2);
        assertEquals(
                "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.union(b1, b2, b3);
        assertEquals(
                "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.union(b1, b4);
        assertEquals(
                "Union(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
                q3.serialize());
    }

    @Test
    public void union0Test() {
        PqlBitmapQuery q = sampleIndex.union();
        assertEquals("Union()", q.serialize());
    }

    @Test
    public void union1Test() {
        PqlBitmapQuery q = sampleIndex.union(sampleField.row(10));
        assertEquals("Union(Bitmap(row=10, field='sample-field'))", q.serialize());
    }

    @Test
    public void intersectTest() {
        PqlBitmapQuery b1 = sampleField.row(10);
        PqlBitmapQuery b2 = sampleField.row(20);
        PqlBitmapQuery b3 = sampleField.row(42);
        PqlBitmapQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.intersect(b1, b2);
        assertEquals(
                "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.intersect(b1, b2, b3);
        assertEquals(
                "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.intersect(b1, b4);
        assertEquals(
                "Intersect(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
                q3.serialize());
    }

    @Test
    public void differenceTest() {
        PqlBitmapQuery b1 = sampleField.row(10);
        PqlBitmapQuery b2 = sampleField.row(20);
        PqlBitmapQuery b3 = sampleField.row(42);
        PqlBitmapQuery b4 = collabField.row(2);

        PqlBaseQuery q1 = sampleIndex.difference(b1, b2);
        assertEquals(
                "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
                q1.serialize());

        PqlBaseQuery q2 = sampleIndex.difference(b1, b2, b3);
        assertEquals(
                "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'), Bitmap(row=42, field='sample-field'))",
                q2.serialize());

        PqlBaseQuery q3 = sampleIndex.difference(b1, b4);
        assertEquals(
                "Difference(Bitmap(row=10, field='sample-field'), Bitmap(row=2, field='collaboration'))",
                q3.serialize());
    }

    @Test
    public void xorTest() {
        PqlBitmapQuery b1 = sampleField.row(10);
        PqlBitmapQuery b2 = sampleField.row(20);
        PqlBaseQuery q1 = sampleIndex.xor(b1, b2);
        assertEquals(
                "Xor(Bitmap(row=10, field='sample-field'), Bitmap(row=20, field='sample-field'))",
                q1.serialize());
    }

    @Test
    public void countTest() {
        PqlBitmapQuery b = collabField.row(42);
        PqlQuery q = projectIndex.count(b);
        assertEquals(
                "Count(Bitmap(row=42, field='collaboration'))",
                q.serialize());
    }

    @Test
    public void topNTest() {
        PqlQuery q1 = sampleField.topN(27);
        assertEquals(
                "TopN(field='sample-field', n=27)",
                q1.serialize());

        PqlQuery q2 = sampleField.topN(10, collabField.row(3));
        assertEquals(
                "TopN(Bitmap(row=3, field='collaboration'), field='sample-field', n=10)",
                q2.serialize());

        PqlBaseQuery q3 = sampleField.topN(12, collabField.row(7), "category", 80, 81);
        assertEquals(
                "TopN(Bitmap(row=7, field='collaboration'), field='sample-field', n=12, field='category', filters=[80,81])",
                q3.serialize());

        PqlBaseQuery q4 = sampleField.topN(5, null);
        assertEquals(
                "TopN(field='sample-field', n=5)",
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
                "Range(row=10, field='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
                q.serialize());
        q = sampleField.range("b7feb014-8ea7-49a8-9cd8-19709161ab63", start.getTime(), end.getTime());
        assertEquals(
                "Range(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='sample-field', start='1970-01-01T00:00', end='2000-02-02T03:04')",
                q.serialize());
    }

    @Test
    public void setRowAttrsTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("active", true);
        PqlQuery q = collabField.setRowAttrs(5, attrsMap);
        assertEquals(
                "SetRowAttrs(row=5, field='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
        q = collabField.setRowAttrs("b7feb014-8ea7-49a8-9cd8-19709161ab63", attrsMap);
        assertEquals(
                "SetRowAttrs(row='b7feb014-8ea7-49a8-9cd8-19709161ab63', field='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
    }

    @Test(expected = PilosaException.class)
    public void setBitmapAttrsInvalidValuesTest() {
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
                "SetColumnAttrs(col=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize());
        q = projectIndex.setColumnAttrs("b7feb014-8ea7-49a8-9cd8-19709161ab63", attrsMap);
        assertEquals(
                "SetColumnAttrs(col='b7feb014-8ea7-49a8-9cd8-19709161ab63', happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
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
                "Sum(Bitmap(row=10, field='collaboration'), field='collaboration')",
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
                "SetValue(col=10, collaboration=20)",
                q.serialize());
    }
}
