# Importing Data

If you have large amounts of data, it is more efficient to import it into Pilosa instead of using multiple SetBit queries. This library supports importing bits into an existing frame.

Before starting the import, create an instance of a struct which implements `BitIterator` interface and pass it to the `client.importFrame` method. This library ships with the `CsvFileBitIterator` class which supports importing bits in the CSV (comma separated values) format:


```
ROW_ID,COLUMN_ID
```

Optionally, a timestamp can be added. Note that Pilosa is not time zone aware:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

Note that each line corresponds to a single bit and ends with a new line (`\n` or `\r\n`).

```java
String data = "1,10,683793200\n" +
              "5,20,683793300\n" +
              "3,41,683793385\n";
InputStream stream = new ByteArrayInputStream(data.getBytes("UTF-8"));
CsvFileBitIterator iterator = CsvFileBitIterator.fromStream(stream);
```

You can use the `CsvFilteBiterator.fromPath` method to read the bits in a file:
```java
CsvFileBitIterator iterator = CsvFileBitIterator.fromPath("/tmp/sample.csv");
```

After creating the iterator, use the `PilosaClient.importFrame` method to start importing:
```java
try {
    client.importFrame(frame, iterator);
}
catch (PilosaException ex) {
    // Handle the error.
}
```

`BitIterator` extends `Iterator` interface, so you can create new iterators by implementing it. Below is a sample iterator which returns prepopulated bits:
```java
class StaticBitIterator implements BitIterator {
    private List<Bit> bits;
    private int index = 0;

    StaticBitIterator() {
        this.bits = new ArrayList<>(3);
        this.bits.add(Bit.create(1, 10, 683793200));
        this.bits.add(Bit.create(5, 20, 683793300));
        this.bits.add(Bit.create(3, 41, 683793385));
    }

    @Override
    public boolean hasNext() {
        return this.index < this.bits.size();
    }

    @Override
    public Bit next() {
        return this.bits.get(index++);
    }

    @Override
    public void remove() {
        // this is just to avoid compilation problems on JDK 7
    }
}
```
