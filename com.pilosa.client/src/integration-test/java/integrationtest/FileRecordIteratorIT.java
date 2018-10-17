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

package integrationtest;

import com.pilosa.client.Column;
import com.pilosa.client.FieldValue;
import com.pilosa.client.IntegrationTest;
import com.pilosa.client.csv.*;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.Record;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class FileRecordIteratorIT {
    @Test
    public void csvRowIDColumnIDDefaultTimestampFormatTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_id-column_id-custom_timestamp.csv",
                new RowIDColumnIDUnserializer(),
                getTargetRowIDColumnIDTimestamp());
    }

    @Test(expected = PilosaException.class)
    public void csvRowIDColumnIDTimestampFailTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_id-column_id-timestamp.csv",
                new RowIDColumnIDUnserializer(),
                getTargetRowIDColumnIDTimestamp());
    }

    @Test
    public void csvRowIDColumnIDTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_id-column_id.csv",
                new RowIDColumnIDUnserializer(),
                getTargetRowIDColumnID());
    }

    @Test
    public void csvRowIDColumnIDTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowIDColumnIDUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_id-column_id-timestamp.csv",
                unserializer,
                getTargetRowIDColumnIDTimestamp());
    }

    @Test
    public void csvRowIDColumnKeyTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowIDColumnKeyUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_id-column_key.csv",
                unserializer,
                getTargetRowIDColumnKey());
    }

    @Test
    public void csvRowIDColumnKeyTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowIDColumnKeyUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_id-column_key-timestamp.csv",
                unserializer,
                getTargetRowIDColumnKeyTimestamp());
    }

    @Test
    public void csvRowKeyColumnIDTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowKeyColumnIDUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_key-column_id.csv",
                unserializer,
                getTargetRowKeyColumnID());
    }

    @Test
    public void csvRowKeyColumnIDTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowKeyColumnIDUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_key-column_id-timestamp.csv",
                unserializer,
                getTargetRowKeyColumnIDTimestamp());
    }

    @Test
    public void csvRowKeyColumnKeyTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_key-column_key.csv",
                new RowKeyColumnKeyUnserializer(),
                getTargetRowKeyColumnKey());
    }

    @Test
    public void csvRowKeyColumnKeyTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowKeyColumnKeyUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_key-column_key-timestamp.csv",
                unserializer,
                getTargetRowKeyColumnKeyTimestamp());
    }

    @Test
    public void csvRowBoolColumnIDTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_bool-column_id.csv",
                new RowBoolColumnIDUnserializer(),
                getTargetRowBoolColumnID());
    }

    @Test
    public void csvRowBoolColumnIDTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowBoolColumnIDUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_bool-column_id-timestamp.csv",
                unserializer,
                getTargetRowBoolColumnIDTimestamp());
    }

    @Test
    public void csvRowBoolColumnKeyTest() throws FileNotFoundException {
        readCompareFromCSV(
                "row_bool-column_key.csv",
                new RowBoolColumnKeyUnserializer(),
                getTargetRowBoolColumnKey());
    }

    @Test
    public void csvRowBoolColumnKeyTimestampTest() throws FileNotFoundException {
        LineUnserializer unserializer = new RowBoolColumnKeyUnserializer();
        unserializer.setTimestampFormat(null);
        readCompareFromCSV(
                "row_bool-column_key-timestamp.csv",
                unserializer,
                getTargetRowBoolColumnKeyTimestamp());
    }

    @Test
    public void csvColumnIDValueTest() throws FileNotFoundException {
        readCompareFromCSV(
                "column_id-value.csv",
                new ColumnIDValueUnserializer(),
                getTargetColumnIDValue());
    }

    @Test
    public void csvColumnKeyValueTest() throws FileNotFoundException {
        readCompareFromCSV(
                "column_key-value.csv",
                new ColumnKeyValueUnserializer(),
                getTargetColumnKeyValue());
    }

    private List<Record> getTargetRowIDColumnID() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(1L, 10L));
        target.add(Column.create(5L, 20L));
        target.add(Column.create(3L, 41L));
        return target;
    }

    private List<Record> getTargetRowIDColumnIDTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(1L, 10L, 683793200L));
        target.add(Column.create(5L, 20L, 683793300L));
        target.add(Column.create(3L, 41L, 683793385L));
        return target;
    }

    private List<Record> getTargetRowIDColumnKey() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(1L, "ten"));
        target.add(Column.create(5L, "twenty"));
        target.add(Column.create(3L, "forty-one"));
        return target;
    }

    private List<Record> getTargetRowIDColumnKeyTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(1L, "ten", 683793200L));
        target.add(Column.create(5L, "twenty", 683793300L));
        target.add(Column.create(3L, "forty-one", 683793385L));
        return target;
    }

    private List<Record> getTargetRowKeyColumnID() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create("one", 10L));
        target.add(Column.create("five", 20L));
        target.add(Column.create("three", 41L));
        return target;
    }

    private List<Record> getTargetRowKeyColumnIDTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create("one", 10L, 683793200L));
        target.add(Column.create("five", 20L, 683793300L));
        target.add(Column.create("three", 41L, 683793385L));
        return target;
    }

    private List<Record> getTargetRowKeyColumnKey() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create("one", "ten"));
        target.add(Column.create("five", "twenty"));
        target.add(Column.create("three", "forty-one"));
        return target;
    }

    private List<Record> getTargetRowKeyColumnKeyTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create("one", "ten", 683793200L));
        target.add(Column.create("five", "twenty", 683793300L));
        target.add(Column.create("three", "forty-one", 683793385L));
        return target;
    }

    private List<Record> getTargetRowBoolColumnID() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(true, 10L));
        target.add(Column.create(false, 20L));
        target.add(Column.create(true, 41L));
        return target;
    }

    private List<Record> getTargetRowBoolColumnIDTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(true, 10, 683793200L));
        target.add(Column.create(false, 20, 683793300L));
        target.add(Column.create(true, 41, 683793385L));
        return target;
    }

    private List<Record> getTargetRowBoolColumnKey() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(true, "ten"));
        target.add(Column.create(false, "twenty"));
        target.add(Column.create(true, "forty-one"));
        return target;
    }

    private List<Record> getTargetRowBoolColumnKeyTimestamp() {
        List<Record> target = new ArrayList<>(3);
        target.add(Column.create(true, "ten", 683793200L));
        target.add(Column.create(false, "twenty", 683793300L));
        target.add(Column.create(true, "forty-one", 683793385L));
        return target;
    }

    private List<Record> getTargetColumnIDValue() {
        List<Record> target = new ArrayList<>(3);
        target.add(FieldValue.create(10L, -100));
        target.add(FieldValue.create(20L, 0));
        target.add(FieldValue.create(41L, 200));
        return target;
    }

    private List<Record> getTargetColumnKeyValue() {
        List<Record> target = new ArrayList<>(3);
        target.add(FieldValue.create("ten", -100));
        target.add(FieldValue.create("twenty", 0));
        target.add(FieldValue.create("forty-one", 200));
        return target;
    }

    private void readCompareFromCSV(String path, LineUnserializer unserializer, List<Record> target)
            throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource(path);
        if (uri == null) {
            fail(String.format("%s not found", path));
        }
        FileRecordIterator iterator = FileRecordIterator.fromPath(uri.getPath(), unserializer);
        List<Record> records = new ArrayList<>(3);
        while (iterator.hasNext()) {
            records.add(iterator.next());
        }
        assertEquals(3, records.size());
        assertEquals(target, records);

        // coverage
        assertFalse(iterator.hasNext());
        iterator.remove();
    }
}
