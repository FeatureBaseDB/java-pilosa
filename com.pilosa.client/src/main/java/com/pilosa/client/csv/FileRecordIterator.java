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

package com.pilosa.client.csv;

import com.pilosa.client.RecordIterator;
import com.pilosa.client.orm.Record;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Scanner;

public class FileRecordIterator implements RecordIterator {
    public static FileRecordIterator fromPath(String path, LineUnserializer unserializer)
            throws FileNotFoundException {
        return fromStream(new FileInputStream(path), unserializer);
    }

    public static FileRecordIterator fromStream(InputStream stream, LineUnserializer unserializer) {
        return new FileRecordIterator(new Scanner(stream), unserializer);
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
        this.nextRecord = unserializer.unserialize(line.split(","));
        return true;
    }

    @Override
    public Record next() {
        return this.nextRecord;
    }

    @Override
    public void remove() {
        // JDK 7 compatibility
    }

    private FileRecordIterator(Scanner scanner, LineUnserializer unserializer) {
        this.scanner = scanner;
        this.unserializer = unserializer;
    }

    private LineUnserializer unserializer = null;
    private Scanner scanner = null;
    private Record nextRecord = null;
}