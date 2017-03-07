package com.pilosa.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * Iterates over a CSV of bitmaps.
 * <p>
 * The CSV file should not have a header and should have the following structure:
 * <pre>
 *     BITMAP_ID,PROFILE_ID[,TIMESTAMP]
 * </pre>
 */
public class CsvFileBitIterator implements IBitIterator {
    private Scanner scanner = null;
    private Bit nextBit = null;

    private CsvFileBitIterator() {
    }

    /**
     * Creates a bit iterator from the CSV file at the given path.
     * @param path of the CSV file
     * @return bit iterator
     * @throws FileNotFoundException if the path does not exist
     */
    public static CsvFileBitIterator fromPath(String path) throws FileNotFoundException {
        return fromStream(new FileInputStream(path));
    }

    /**
     * Creates a bit iterator from an input stream.
     *
     * @param stream CSV stream
     * @return bit iterator
     */
    public static CsvFileBitIterator fromStream(InputStream stream) {
        CsvFileBitIterator iterator = new CsvFileBitIterator();
        iterator.scanner = new Scanner(stream);
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
                this.nextBit = Bit.create(bitmapID, profileID, timestamp);
                return true;
            }
        }
        scanner.close();
        this.scanner = null;
        return false;
    }

    @Override
    public Bit next() {
        return this.nextBit;
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }
}
