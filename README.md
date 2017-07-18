# RasterFrames

To initialize the GeoTrellis support, call the following function in your Spark setup code: 

    rfInit(sqlContext)

A smattering of functions currentlly available via Spark Dataframes (prepend `st_` for access to SQL variants):

* `randomTile(columns, rows, cellType)` - Create a tile with random cell values.
* `explodeTiles(tileColumn)` - Create a row for each pixel in tile.
* `explodeTileSample(sampleFraction, tileColumn)` - Create a row for each pixel in tile with random sampling.
* `tileRows(tileColumn)` - Query the number of rows in a tile.
* `tileCols(tileColumn)` - Query the number of columns in a tile.
* `focalSum(tileColumn, radius)` - Compute the focal sum of a tile with the given radius.
* `localMax(tileColumn)` - Compute the cellwise/local max operation between tiles in a column.
* `localMin(tileColumn)` - Compute the cellwise/local min operation between tiles in a column.
* `localAdd(leftTileColumn, rightTileColumn)` - Cellwise addition between two tiles.
* `localSubtract(leftTileColumn, rightTileColumn)` -Cellwise subtraction between two tiles.
* `localAlgebra(localTileBinaryOp, leftTileColumn, rightTileColumn)` - Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two tile columns.
* `tileHistogram(tileColumn)` - Compute the integral value histogram of cellv values in each tile.tile values.
* `tileHistogramDouble(tileColumn)` - Compute the floating point histogram of cellv values in each tile.
* `tileStatistics(tileColumn)` - Compute min, max, mean, stddev, median, mode, etc. of integral cell values.tile values.
* `tileStatsDouble(tileColumn)` - Compute min, max, mean, stddev, median, mode, etc. of floating point cell values.
* `tileMean(tileColumn)` - Compute the mean of all integral cell values in each tile.
* `tileMeanDouble(tileColumn)` - Compute the mean of all floating point cell values in each tile.
* `localStats(tileColumn)` -   Compute cell-local aggregate descriptive statistics for a column of tiles.
* `aggHistogram(tileColumn)` - Compute the full column aggregate floating point histogram.
* `aggStats(tileColumn)` - Compute the full column aggregate floating point summary statistics.
* `renderAscii(tileColumn)` - Render tile as ASCII string for debugging purposes.
