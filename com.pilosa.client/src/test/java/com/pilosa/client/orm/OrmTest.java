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
    private Field projectKeyField;

    {
        this.projectIndex = Index.create("project-db");
        FieldOptions fieldOptions = FieldOptions.withDefaults();
        this.collabField = projectIndex.field("collaboration", fieldOptions);
        fieldOptions = FieldOptions.builder()
                .keys(true)
                .build();
        this.projectKeyField = projectIndex.field("key", fieldOptions);
    }

    @Test
    public void pqlQueryCreate() {
        PqlBaseQuery pql = new PqlBaseQuery("Bitmap(field='foo', row=10)");
        assertEquals("Bitmap(field='foo', row=10)", pql.serialize().getQuery());
        assertEquals(null, pql.getIndex());
    }

    @Test
    public void pqlRowQueryCreate() {
        PqlRowQuery pql = new PqlRowQuery("Bitmap(field='foo', row=10)");
        assertEquals("Bitmap(field='foo', row=10)", pql.serialize().getQuery());
        assertEquals(null, pql.getIndex());
    }

    @Test
    public void batchTest() {
        PqlBatchQuery b = sampleIndex.batchQuery();
        b.add(sampleField.row(44));
        b.add(sampleField.row(10101));
        assertEquals(
                "Row(sample-field=44)Row(sample-field=10101)",
                b.serialize().getQuery());
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
                b.serialize().getQuery());
    }

    @Test(expected = PilosaException.class)
    public void batchAddFailsForDifferentDbsTest() {
        PqlBatchQuery b = projectIndex.batchQuery();
        b.add(sampleField.row(1));
    }

    @Test
    public void rowTest() {
        PqlBaseQuery q = collabField.row(5);
        assertEquals(
                "Row(collaboration=5)",
                q.serialize().getQuery());

        q = projectKeyField.row("some_id");
        assertEquals(
                "Row(key='some_id')",
                q.serialize().getQuery());

        q = collabField.row(true);
        assertEquals(
                "Row(collaboration=true)",
                q.serialize().getQuery());
    }

    @Test
    public void setTest() {
        PqlQuery q = collabField.set(5, 10);
        assertEquals(
                "Set(10,collaboration=5)",
                q.serialize().getQuery());

        q = collabField.set(5, "ten");
        assertEquals(
                "Set('ten',collaboration=5)",
                q.serialize().getQuery());

        q = collabField.set("five", 10);
        assertEquals(
                "Set(10,collaboration='five')",
                q.serialize().getQuery());

        q = collabField.set("five", "10");
        assertEquals(
                "Set('10',collaboration='five')",
                q.serialize().getQuery());

        q = collabField.set(true, 10);
        assertEquals(
                "Set(10,collaboration=true)",
                q.serialize().getQuery());

        q = collabField.set(true, "ten");
        assertEquals(
                "Set('ten',collaboration=true)",
                q.serialize().getQuery());
    }

    @Test
    public void setWithTimestampTest() {
        Calendar timestamp = Calendar.getInstance();
        timestamp.set(2017, Calendar.APRIL, 24, 12, 14);
        PqlQuery q = collabField.set(10, 20, timestamp.getTime());
        assertEquals(
                "Set(20,collaboration=10,2017-04-24T12:14)",
                q.serialize().getQuery());

        q = collabField.set(10, "twenty", timestamp.getTime());
        assertEquals(
                "Set('twenty',collaboration=10,2017-04-24T12:14)",
                q.serialize().getQuery());

        q = collabField.set("ten", 20, timestamp.getTime());
        assertEquals(
                "Set(20,collaboration='ten',2017-04-24T12:14)",
                q.serialize().getQuery());

        q = collabField.set("ten", "twenty", timestamp.getTime());
        assertEquals(
                "Set('twenty',collaboration='ten',2017-04-24T12:14)",
                q.serialize().getQuery());

        q = collabField.set(true, 20, timestamp.getTime());
        assertEquals(
                "Set(20,collaboration=true,2017-04-24T12:14)",
                q.serialize().getQuery());

        q = collabField.set(true, "twenty", timestamp.getTime());
        assertEquals(
                "Set('twenty',collaboration=true,2017-04-24T12:14)",
                q.serialize().getQuery());

    }

    @Test
    public void clearTest() {
        PqlQuery q = collabField.clear(5, 10);
        assertEquals(
                "Clear(10,collaboration=5)",
                q.serialize().getQuery());

        q = collabField.clear(5, "ten");
        assertEquals(
                "Clear('ten',collaboration=5)",
                q.serialize().getQuery());

        q = collabField.clear("five", 10);
        assertEquals(
                "Clear(10,collaboration='five')",
                q.serialize().getQuery());

        q = collabField.clear("five", "10");
        assertEquals(
                "Clear('10',collaboration='five')",
                q.serialize().getQuery());

        q = collabField.clear(true, 10);
        assertEquals(
                "Clear(10,collaboration=true)",
                q.serialize().getQuery());

        q = collabField.clear(true, "ten");
        assertEquals(
                "Clear('ten',collaboration=true)",
                q.serialize().getQuery());
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
                q1.serialize().getQuery());

        PqlBaseQuery q2 = sampleIndex.union(b1, b2, b3);
        assertEquals(
                "Union(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize().getQuery());

        PqlBaseQuery q3 = sampleIndex.union(b1, b4);
        assertEquals(
                "Union(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize().getQuery());
    }

    @Test
    public void union0Test() {
        PqlRowQuery q = sampleIndex.union();
        assertEquals("Union()", q.serialize().getQuery());
    }

    @Test
    public void union1Test() {
        PqlRowQuery q = sampleIndex.union(sampleField.row(10));
        assertEquals("Union(Row(sample-field=10))", q.serialize().getQuery());
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
                q1.serialize().getQuery());

        PqlBaseQuery q2 = sampleIndex.intersect(b1, b2, b3);
        assertEquals(
                "Intersect(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize().getQuery());

        PqlBaseQuery q3 = sampleIndex.intersect(b1, b4);
        assertEquals(
                "Intersect(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize().getQuery());
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
                q1.serialize().getQuery());

        PqlBaseQuery q2 = sampleIndex.difference(b1, b2, b3);
        assertEquals(
                "Difference(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
                q2.serialize().getQuery());

        PqlBaseQuery q3 = sampleIndex.difference(b1, b4);
        assertEquals(
                "Difference(Row(sample-field=10),Row(collaboration=2))",
                q3.serialize().getQuery());
    }

    @Test
    public void xorTest() {
        PqlRowQuery b1 = sampleField.row(10);
        PqlRowQuery b2 = sampleField.row(20);
        PqlBaseQuery q1 = sampleIndex.xor(b1, b2);
        assertEquals(
                "Xor(Row(sample-field=10),Row(sample-field=20))",
                q1.serialize().getQuery());
    }

    @Test
    public void notTest() {
        PqlRowQuery row = sampleField.row(10);
        PqlBaseQuery q = sampleIndex.not(sampleField.row(1));
        assertEquals(
                "Not(Row(sample-field=1))",
                q.serialize().getQuery());
    }

    @Test
    public void countTest() {
        PqlRowQuery b = collabField.row(42);
        PqlQuery q = projectIndex.count(b);
        assertEquals(
                "Count(Row(collaboration=42))",
                q.serialize().getQuery());
    }

    @Test
    public void topNTest() {
        PqlQuery q1 = collabField.topN(27);
        assertEquals(
                "TopN(collaboration,n=27)",
                q1.serialize().getQuery());

        PqlQuery q2 = collabField.topN(10, collabField.row(3));
        assertEquals(
                "TopN(collaboration,Row(collaboration=3),n=10)",
                q2.serialize().getQuery());

        PqlBaseQuery q3 = sampleField.topN(12, collabField.row(7), "category", 80, 81);
        assertEquals(
                "TopN(sample-field,Row(collaboration=7),n=12,attrName='category',attrValues=[80,81])",
                q3.serialize().getQuery());

        PqlBaseQuery q4 = sampleField.topN(12, null, "category", 80, 81);
        assertEquals(
                "TopN(sample-field,n=12,attrName='category',attrValues=[80,81])",
                q4.serialize().getQuery());
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
                q.serialize().getQuery());
        q = collabField.range("foo", start.getTime(), end.getTime());
        assertEquals(
                "Range(collaboration='foo',1970-01-01T00:00,2000-02-02T03:04)",
                q.serialize().getQuery());
    }

    @Test
    public void setRowAttrsTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("active", true);
        PqlQuery q = collabField.setRowAttrs(5, attrsMap);
        assertEquals(
                "SetRowAttrs(collaboration,5,active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize().getQuery());
        q = collabField.setRowAttrs("foo", attrsMap);
        assertEquals(
                "SetRowAttrs('collaboration','foo',active=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize().getQuery());
    }

    @Test(expected = PilosaException.class)
    public void setRowAttrsInvalidValuesTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("color", "blue");
        attrsMap.put("happy", new Object());
        collabField.setRowAttrs(5, attrsMap);
    }

    @Test
    public void optionsTest() {
        OptionsOptions opts = OptionsOptions.builder()
                .build();
        PqlQuery q = projectIndex.options(collabField.row(5), opts);
        assertEquals(
                "Options(Row(collaboration=5),columnAttrs=false,excludeColumns=false,excludeRowAttrs=false)",
                q.serialize().getQuery());

        opts = OptionsOptions.builder()
                .setColumnAttrs(true)
                .setExcludeColumns(true)
                .setExcludeRowAttrs(true)
                .build();
        q = projectIndex.options(collabField.row(5), opts);
        assertEquals(
                "Options(Row(collaboration=5),columnAttrs=true,excludeColumns=true,excludeRowAttrs=true)",
                q.serialize().getQuery());

        opts = OptionsOptions.builder()
                .setColumnAttrs(true)
                .setExcludeColumns(true)
                .setExcludeRowAttrs(true)
                .setShards(1, 3)
                .build();
        q = projectIndex.options(collabField.row(5), opts);
        assertEquals(
                "Options(Row(collaboration=5),columnAttrs=true,excludeColumns=true,excludeRowAttrs=true,shards=[1,3])",
                q.serialize().getQuery());
    }


    @Test
    public void setColumnAttrsTest() {
        Map<String, Object> attrsMap = new TreeMap<>();
        attrsMap.put("quote", "\"Don't worry, be happy\"");
        attrsMap.put("happy", true);
        PqlQuery q = projectIndex.setColumnAttrs(5, attrsMap);
        assertEquals(
                "SetColumnAttrs(5,happy=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize().getQuery());
        q = projectIndex.setColumnAttrs("b7feb014-8ea7-49a8-9cd8-19709161ab63", attrsMap);
        assertEquals(
                "SetColumnAttrs('b7feb014-8ea7-49a8-9cd8-19709161ab63',happy=true,quote=\"\\\"Don't worry, be happy\\\"\")",
                q.serialize().getQuery());
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
                q.serialize().getQuery());
    }

    @Test
    public void fieldLessThanOrEqualTest() {
        PqlQuery q = collabField.lessThanOrEqual(10);
        assertEquals(
                "Range(collaboration <= 10)",
                q.serialize().getQuery());

    }

    @Test
    public void fieldGreaterThanTest() {
        PqlQuery q = collabField.greaterThan(10);
        assertEquals(
                "Range(collaboration > 10)",
                q.serialize().getQuery());

    }

    @Test
    public void fieldGreaterThanOrEqualTest() {
        PqlQuery q = collabField.greaterThanOrEqual(10);
        assertEquals(
                "Range(collaboration >= 10)",
                q.serialize().getQuery());

    }

    @Test
    public void fieldEqualsTest() {
        PqlQuery q = collabField.equals(10);
        assertEquals(
                "Range(collaboration == 10)",
                q.serialize().getQuery());
    }

    @Test
    public void fieldNotEqualsTest() {
        PqlQuery q = collabField.notEquals(10);
        assertEquals(
                "Range(collaboration != 10)",
                q.serialize().getQuery());
    }

    @Test
    public void fieldNotNullTest() {
        PqlQuery q = collabField.notNull();
        assertEquals(
                "Range(collaboration != null)",
                q.serialize().getQuery());
    }

    @Test
    public void fieldBetweenTest() {
        PqlQuery q = collabField.between(10, 20);
        assertEquals(
                "Range(collaboration >< [10,20])",
                q.serialize().getQuery());

    }

    @Test
    public void fieldSumTest() {
        PqlQuery q = collabField.sum(collabField.row(10));
        assertEquals(
                "Sum(Row(collaboration=10),field='collaboration')",
                q.serialize().getQuery());
        q = collabField.sum();
        assertEquals(
                "Sum(field='collaboration')",
                q.serialize().getQuery()
        );
    }

    @Test
    public void fieldSetValueTest() {
        PqlQuery q = collabField.setValue(10, 20);
        assertEquals(
                "Set(10, collaboration=20)",
                q.serialize().getQuery());
        q = collabField.setValue("foo", 100);
        assertEquals("Set('foo', collaboration=100)",
                q.serialize().getQuery());
    }

    @Test
    public void fieldStoreTest() {
        PqlQuery q = sampleField.store(collabField.row(5), 10);
        assertEquals(
                "Store(Row(collaboration=5),sample-field=10)",
                q.serialize().getQuery());
        q = sampleField.store(collabField.row("five"), "ten");
        assertEquals(
                "Store(Row(collaboration='five'),sample-field='ten')",
                q.serialize().getQuery());
    }

    @Test
    public void fieldClearRowTest() {
        PqlQuery q = collabField.clearRow(5);
        assertEquals(
                "ClearRow(collaboration=5)",
                q.serialize().getQuery());

        q = collabField.clearRow("five");
        assertEquals(
                "ClearRow(collaboration='five')",
                q.serialize().getQuery());

        q = collabField.clearRow(true);
        assertEquals(
                "ClearRow(collaboration=true)",
                q.serialize().getQuery());
    }
}
