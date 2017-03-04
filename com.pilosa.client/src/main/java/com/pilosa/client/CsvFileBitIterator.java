package com.pilosa.client;

import com.pilosa.client.internal.ClientProtos;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class CsvFileBitIterator implements IBitIterator {
    private Scanner scanner = null;
    private ClientProtos.Bit nextBit = null;

    private CsvFileBitIterator() {
    }

    public static CsvFileBitIterator fromPath(String path) throws FileNotFoundException {
        CsvFileBitIterator iterator = new CsvFileBitIterator();
        iterator.scanner = new Scanner(new FileInputStream(path));
        return iterator;
    }

    @Override
    public boolean hasNext() {
        if (this.scanner == null) {
            return false;
        }
        if (this.scanner.hasNextLine()) {
            String line = this.scanner.nextLine();
            if (!line.isEmpty()) {
                String[] fields = line.split(",");
                long bitmapID = Long.parseLong(fields[0]);
                long profileID = Long.parseLong(fields[1]);
                long timestamp = 0;
                if (fields.length > 2) {
                    timestamp = Long.parseLong(fields[2]);
                }
                this.nextBit = ClientProtos.Bit.newBuilder()
                        .setBitmapID(bitmapID)
                        .setProfileID(profileID)
                        .setTimestamp(timestamp)
                        .build();
                return true;
            }
        }
        scanner.close();
        this.scanner = null;
        return false;
    }

    @Override
    public ClientProtos.Bit next() {
        return this.nextBit;
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }
}
