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

package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.TimeZone;

/**
 * Iterates over a CSV of rows.
 * <p>
 * This class is used for iterating over rows for a import operation.
 * <p>
 * The CSV file should not have a header and should have the following structure:
 * <pre>
 *     ROW_ID,COLUMN_ID[,TIMESTAMP]
 * </pre>
 * @see <a href="https://www.pilosa.com/docs/administration/#importing-and-exporting-data/">Importing and Exporting Data</a>
 */
public class CsvFileColumnIterator implements ColumnIterator {
    private CsvFileColumnIterator(SimpleDateFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
        if (this.timestampFormat != null) {
            this.timestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

    }

    private long parseTimestamp(final String s) {
        if (this.timestampFormat == null) {
            return Long.parseLong(s);
        }
        try {
            Date date = this.timestampFormat.parse(s);
            return date.getTime() / 1000;
        } catch (ParseException ex) {
            throw new PilosaException("Error parsing timestamp:", ex);
        }
    }

    /**
     * Creates a bit iterator from the CSV file at the given path.
     *
     * @param path of the CSV file
     * @return bit iterator
     * @throws FileNotFoundException if the path does not exist
     */
    public static CsvFileColumnIterator fromPath(String path) throws FileNotFoundException {
        return fromStream(new FileInputStream(path));
    }

    /**
     * Creates a bit iterator from the CSV file at the given path.
     *
     * @param path            of the CSV file
     * @param timestampFormat timestamp format to be used for CSV lines if they have the timestamp field
     * @return bit iterator
     * @throws FileNotFoundException if the path does not exist
     */
    public static CsvFileColumnIterator fromPath(String path, SimpleDateFormat timestampFormat) throws FileNotFoundException {
        return fromStream(new FileInputStream(path), timestampFormat);
    }

    /**
     * Creates a bit iterator from an input stream.
     *
     * @param stream CSV stream
     * @return bit iterator
     */
    @SuppressWarnings("WeakerAccess")
    public static CsvFileColumnIterator fromStream(InputStream stream) {
        return fromStream(stream, null);
    }

    /**
     * Creates a bit iterator from an input stream.
     *
     * @param stream          CSV stream
     * @param timestampFormat timestamp format to be used for CSV lines if they have the timestamp field
     * @return bit iterator
     */
    @SuppressWarnings("WeakerAccess")
    public static CsvFileColumnIterator fromStream(InputStream stream, SimpleDateFormat timestampFormat) {
        CsvFileColumnIterator iterator = new CsvFileColumnIterator(timestampFormat);
        iterator.scanner = new Scanner(stream);
        return iterator;
    }

    public static SimpleDateFormat getDefaultTimestampFormat() {
        return defaultTimestampFormat;
    }

    @Override
    public boolean hasNext() {
        if (this.scanner == null || !this.scanner.hasNextLine()) {
            return false;
        }
        String line = this.scanner.nextLine();
        if (line.isEmpty()) {
            this.scanner.close();
            this.scanner = null;
            return false;
        }
        String[] fields = line.split(",");
        long rowID = Long.parseLong(fields[0]);
        long columnID = Long.parseLong(fields[1]);
        long timestamp = 0;
        if (fields.length > 2) {
            timestamp = parseTimestamp(fields[2]);
        }
        this.nextColumn = Column.create(rowID, columnID, timestamp);
        return true;
    }

    @Override
    public Column next() {
        return this.nextColumn;
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }

    private Scanner scanner = null;
    private Column nextColumn = null;
    private SimpleDateFormat timestampFormat;
    private final static SimpleDateFormat defaultTimestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
}
