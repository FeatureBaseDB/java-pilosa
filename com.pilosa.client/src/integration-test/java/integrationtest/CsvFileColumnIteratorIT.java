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
import com.pilosa.client.CsvFileColumnIterator;
import com.pilosa.client.IntegrationTest;
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class CsvFileColumnIteratorIT {
    @Test
    public void readFromCsvTest() throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource("sample1.csv");
        if (uri == null) {
            fail("sample1.csv not found");
        }
        CsvFileColumnIterator iterator = CsvFileColumnIterator.fromPath(uri.getPath());
        List<Column> columns = new ArrayList<>(3);
        while (iterator.hasNext()) {
            columns.add(iterator.next());
        }
        List<Column> target = getTargetRows();
        assertEquals(3, columns.size());
        assertEquals(target, columns);

        // to get %100 test coverage
        assertFalse(iterator.hasNext());
        iterator.remove();
    }

    @Test
    public void readFromCsvWithCustomTimestampTest() throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource("sample2.csv");
        if (uri == null) {
            fail("sample2.csv not found");
        }
        SimpleDateFormat timestampFormat = CsvFileColumnIterator.getDefaultTimestampFormat();
        CsvFileColumnIterator iterator = CsvFileColumnIterator.fromPath(uri.getPath(), timestampFormat);
        List<Column> columns = new ArrayList<>(3);
        while (iterator.hasNext()) {
            columns.add(iterator.next());
        }
        List<Column> target = getTargetRows();
        assertEquals(3, columns.size());
        assertEquals(target, columns);
    }

    @Test(expected = PilosaException.class)
    public void readFromCsvWithCustomTimestampFailTest() throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource("sample2.csv");
        if (uri == null) {
            fail("sample2.csv not found");
        }
        // timestamp format that doesn't match sample2.csv
        SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'TX'hh:mm");
        CsvFileColumnIterator iterator = CsvFileColumnIterator.fromPath(uri.getPath(), timestampFormat);
        List<Column> columns = new ArrayList<>(3);
        while (iterator.hasNext()) {
            columns.add(iterator.next());
        }
    }

    private List<Column> getTargetRows() {
        List<List<Long>> targetValues = Arrays.asList(
                Arrays.asList(1L, 10L, 683793200L),
                Arrays.asList(5L, 20L, 683793300L),
                Arrays.asList(3L, 41L, 683793385L));
        List<Column> target = new ArrayList<>(3);
        for (List<Long> item : targetValues) {
            target.add(Column.create(item.get(0), item.get(1), item.get(2)));
        }
        return target;
    }
}
