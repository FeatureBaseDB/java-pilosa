# Importing Data

If you have large amounts of data, it is more efficient to import it into Pilosa instead of using multiple `Set` or `Clear` queries. This library supports importing columns and field values into an existing field.

The `com.pilosa.client.orm.Record` interface represents a column or a field value.  A *field value* consists of a column ID/Key and an integer value and used with `int` fields. Columns represent a row ID/key, row ID/key/boolean pair and an optional timestamp and used with other field types, such as `set`, `mutex` and `bool`.

Before starting the import, create an instance of an object which implements `com.pilosa.client.RecordIterator` interface and pass it to the `client.importField` method. The iterator must return the correct type (`com.pilosa.client.Column` or `com.pilosa.client.FieldValue` depending on the target field type.)  at its `next` method.

This library ships with the `com.pilosa.client.csv.FileRecordIterator` class which supports importing columns in the CSV (comma separated values) using the following formats for `set` fields:

* Column with row ID/column ID, when both field option keys and index option keys are `false`:
    ```
    ROW_ID,COLUMN_ID
    ```

* Column with row ID/column key, when field option keys is `false` and index option keys is `true`:
    ```
    ROW_ID,COLUMN_KEY
    ```

* Column with row key/column ID, when field option keys is `true` and index option keys is `false`:
    ```
    ROW_KEY,COLUMN_ID
    ```

* Column with row key/column key, when both field option keys and index option keys are `true`:
    ```
    ROW_KEY,COLUMN_KEY
    ```

Following CSV format is supported for `bool` fields. Boolean fields should have `0` for `false` and `1` for `true` values:

* Column with row bool/column ID, when index option keys is `false`:
    ```
    BOOLEAN,COLUMN_ID
    ```

* Column with row bool/column ID, when index option keys is `false`:
    ```
    BOOLEAN,COLUMN_ID
    ```

Optionally, a timestamp can be added. Note that Pilosa is not time zone aware:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

CSV formats below are supported for `int` fields:

* Field value with column ID, when index option keys is `false`:
    ```
    COLUMN_ID,VALUE
    ```

* Field value with column key, when index option keys is `true`:
    ```
    COLUMN_KEY,VALUE
    ```

Note that each line corresponds to a single column and ends with a new line (`\n` or `\r\n`).

Here is some sample code which imports columns without timestamps from a stream:

```java
String data = "1,10\n" +
              "5,20\n" +
              "3,41\n";
InputStream stream = new ByteArrayInputStream(data.getBytes("UTF-8"));
FileIterator iterator = FileRecordIterator.fromStream(stream, field);
```

The default timestamp format is `"yyyy-MM-dd'T'hh:mm:ss"`. In order to use a different format, you can set the third parameter of `FileRecordIterator.fromStream` or `FileRecordIterator.fromPath`. Setting the format parameter to `null` sets the time format to timestamp, shown in the sample below:

_**Note that timestamps should be in nanoseconds, shorter numbers will not be set on the Pilosa server.**_

```java
String data = "1,10,6837932000000000000\n" +
              "5,20,6837933000000000000\n" +
              "3,41,6837933850000000000\n";
InputStream stream = new ByteArrayInputStream(data.getBytes("UTF-8"));
FileIterator iterator = FileRecordIterator.fromStream(stream, field, null);
```

You can use the `FileRecordIterator.fromPath` method to read the columns in a file:
```java
FileRecordIterator iterator = FileRecordIterator.fromPath("/tmp/sample.csv", field);
```

After creating the iterator, use the `PilosaClient.importField` method to start importing:
```java
try {
    client.importField(field, iterator);
}
catch (PilosaException ex) {
    // Handle the error.
}
```

`RecordIterator` extends `Iterator` interface, so you can create new iterators by implementing it. Below is a sample iterator which returns prepopulated columns:

_**Note that timestamps should be in nanoseconds, shorter numbers will not be set on the Pilosa server.**_

```java
class StaticColumnIterator implements RecordIterator {
    private List<Column> columns;
    private int index = 0;

    StaticColumnIterator() {
        this.columns = new ArrayList<>(3);
        this.columns.add(Column.create(1, 10, 6837932000000000000));
        this.columns.add(Column.create(5, 20, 6837933000000000000));
        this.columns.add(Column.create(3, 41, 6837933850000000000));
    }

    @Override
    public boolean hasNext() {
        return this.index < this.columns.size();
    }

    @Override
    public Record next() {
        return this.columns.get(index++);
    }

    @Override
    public void remove() {
        // this is just to avoid compilation problems on JDK 7
    }
}
```

Pilosa supports an optimized way of importing Row ID/Column ID style data by packing them in a roaring bitmap. You can enable optimized imports by using `setRoaring(true)` import option:
```java
ImportOptions importOptions = ImportOptions.builder()
    .setRoaring(true)
    .build();
client.importField(field, iterator, importOptions);
```

Other import options are:
* `setClear(bool)`: `Clear` columns instead of `Set`ting them,
* `setBatchSize(int)`: Sets the number of items read from an iterator before posting them to Pilosa,
* `setThreadCount(int)`: Number of threads to use while importing data.
