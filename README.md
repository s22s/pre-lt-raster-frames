# geotrellis-spark-sql

 [ ![Download](https://api.bintray.com/packages/s22s/maven/geotrellis-spark-sql/images/download.svg) ](https://bintray.com/s22s/maven/geotrellis-spark-sql/_latestVersion)

[![Build Status](https://travis-ci.org/s22s/geotrellis-spark-sql.svg?branch=master)](https://travis-ci.org/s22s/geotrellis-spark-sql)

Experimental facility for encoding [GeoTrellis](https://geotrellis.io/) types into Spark Datasets/Dataframes. 

For usage examples, see the [test specification](src/test/scala/org/apache/spark/sql/gt/GTSQLSpec.scala).

A smattering of functions currentlly available via Spark Dataframes. (Prepend `st_` for access to SQL variants):

* `randomTile(columns, rows, cellType)` - Create a tile with random cell values.
* `explodeTile(tileColumn)` - Create a row for each pixel in tile.
* `explodeAndSampleTile(sampleFraction, tileColumn)` - Create a row for each pixel in tile with random sampling.
* `gridRows(tileColumn)` - Query the number of rows in a tile.
* `gridCols(tileColumn)` - Query the number of columns in a tile.
* `focalSum(tileColumn, radius)` - Compute the focal sum of a tile with the given radius.
* `localMax(tileColumn)` - Compute the cellwise/local max operation between tiles in a column.
* `localMin(tileColumn)` - Compute the cellwise/local min operation between tiles in a column.
* `localAdd(leftTileColumn, rightTileColumn)` - Cellwise addition between two tiles.
* `localSubtract(leftTileColumn, rightTileColumn)` -Cellwise subtraction between two tiles.
* `localAlgebra(localTileBinaryOp, leftTileColumn, rightTileColumn)` - Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two tile columns.
* `tileHistogram(tileColumn)` - Compute the integral value histogram of cellv values in each tile.tile values.
* `tileHistogramDouble(tileColumn)` - Compute the floating point histogram of cellv values in each tile.
* `tileStatistics(tileColumn)` - Compute min, max, mean, stddev, median, mode, etc. of integral cell values.tile values.
* `tileStatisticsDouble(tileColumn)` - Compute min, max, mean, stddev, median, mode, etc. of floating point cell values.
* `tileMean(tileColumn)` - Compute the mean of all integral cell values in each tile.
* `tileMeanDouble(tileColumn)` - Compute the mean of all floating point cell values in each tile.
* `localStats(tileColumn)` -   Compute cell-local aggregate descriptive statistics for a column of tiles.
* `histogram(tileColumn)` - Compute the full column aggregate floating point histogram.
* `renderAscii(tileColumn)` - Render tile as ASCII string for debugging purposes.
