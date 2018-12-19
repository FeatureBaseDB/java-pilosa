## Change Log

* **v1.2.0** (2018-12-19)
    * **Compatible with Pilosa 1.2**
    * Supports imports involving keys.
    * Added support for mutex and bool fields.
    * Added `index.Options`, `field.ClearRow` and `field.Store` functions to support the corresponding PQL calls.
    * Added `com.pilosa.client.csv` package.
    * Added support for roaring importinging `RowIDColumnID` with timestamp data.
    * Updated `com.pilosa.roaring` dependency for improved memory usage.
    * Improved import speed.
    * Fixed schema synchronization.
    * Deprecated: `indexOptions.keys`, use `indexOptions.setKeys` instead.
    * Deprecated: `indexOptions.trackExistence`, use `indexOptions.setTrackExistence` instead.
    * Deprecated: `fieldOptions.keys`, use `fieldOptions.setKeys` instead.

* **v1.0.2** (2018-10-12)
    * Added `trackExistence` index option.
    * Added `not` index method to support `Not` queries. The corresponding index must be created with `trackExistence=true` option. This feature requires Pilosa on master branch.
    * Added support for roaring imports which can speed up the import process by %30 for non-key column imports. Pass `setRoaring(true)`` to `ImportOptions.builder()` to enable it. This feature requires Pilosa on master branch.
    * Fixes: `Column.create` method. See: https://github.com/pilosa/java-pilosa/pull/127

* **v1.0.1** (2018-09-12)
    * Compatible with Pilosa 1.0.
    * Added key import support.
    * Fixed: https://github.com/pilosa/java-pilosa/issues/108
    * Fixed: https://github.com/pilosa/java-pilosa/issues/112
    * Fixed: https://github.com/pilosa/java-pilosa/issues/117

* **v1.0.0** (2018-06-28)
    * Compatible with Pilosa 1.0.
    * Following terminology was changed:
        * frame to field
        * bitmap to row
        * bit to column
        * slice to shard
    * There are three types of fields:
        * Set fields to store boolean values (default)
        * Integer fields to store an integer in the given range.
        * Time fields which can store timestamps.
    * Added `keys` field option.
    * Experimental: Import strategies are experimental and may be removed in later versions.
    * Removed all deprecated code.
    * Removed `Field` type and renamed `Frame` to `Field`.

* **v0.9.0** (2018-05-08)
    * Compatible with Pilosa 0.9.
    * Supports multi-threaded imports and import progress tracking.
    * Added `RangeField.min` and `RangeField.max` methods.
    * **Deprecation** `inverseEnabled` frame option, `Frame.inverseBitmap`, `Frame.inverseTopN`, `Frame.inverseRange` methods. Inverse frames will be removed on Pilosa 1.0.


* **v0.8.2** (2018-02-28)
    * Compatible with Pilosa master, **not compatible with Pilosa 0.8.x releases**.
    * Checks the server version for Pilosa server compatibility. You can call `clientOptions.setSkipVersionCheck()` to disable that.

* **v0.8.1** (2018-01-18)
    * Added `equals`, `notEquals` and `notNull` field operations.
    * **Removal** `TimeQuantum` for `IndexOptions`. Use `TimeQuantum` of individual `FrameOptions` instead.
    * **Removal** `IndexOptions` class is deprecated and will be removed in the future.
    * **Removal** `schema.Index(name, indexOptions)` method.
    * **Removal** column labels and row labels.

* **v0.8.0** (2017-11-16):
    * Added IPv6 support.

* **v0.7.0** (2017-10-04):
    * Added support for creating range encoded frames.
    * Added `Xor` call.
    * Added range field operations.
    * Added support for excluding bits or attributes from bitmap calls. In order to exclude bits, call `setExcludeBits(true)` in your `QueryOptions.Builder`. In order to exclude attributes, call `setExcludeAttributes(true)`.
    * Customizable CSV time stamp format.
    * `HTTPS connections are supported.
    * **Deprecation** Row and column labels are deprecated, and will be removed in a future release of this library. Do not use `IndexOptions.Builder.setColumnLabel` and `FrameOptions.Builder.setRowLabel` methods for new code. See: https://github.com/pilosa/pilosa/issues/752 for more info.

* **v0.5.1** (2017-08-11):
    * Fixes `filters` parameter of the `TopN` parameter.
    * Fixes reading schemas with no indexes.

* **v0.5.0** (2017-08-03):
    * Failover for connection errors.
    * More logging.
    * Uses slf4j instead of log4j for logging.
    * Introduced schemas. No need to re-define already existing indexes and frames.
    * *make* commands are supported on Windows.
    * * *Breaking Change*: Removed `timeQuantum` query option.
    * **Deprecation** `Index.withName` constructor. Use `schema.index` instead.
    * **Deprecation** `client.createIndex`, `client.createFrame`, `client.ensureIndex`, `client.ensureFrame`. Use schemas and `client.syncSchema` instead.

* **v0.4.0** (2017-06-09):
    * Supports Pilosa Server v0.4.0.
    * *Breaking Change*: Renamed `BatchQuery` to `PqlBatchQuery`.
    * Updated the accepted values for index, frame names and labels to match with the Pilosa server.
    * `Union` queries accept 0 or more arguments. `Intersect` and `Difference` queries accept 1 or more arguments.
    * Added `inverse TopN` and `inverse Range` calls.
    * Inverse enabled status of frames is not checked on the client side.

* **v0.3.2** (2017-05-02):
    * Available on Maven Repository.

* **v0.3.1** (2017-05-01):
    * Initial version
    * Supports Pilosa Server v0.3.1.
