# Reference

API Scaladoc can be found [here](latest/api/index.html).

Here's a preliminary list of RasterFrame UDFs available. For the most up to date list, look at the source code for `ColumnFunctions`. These UDFs are also registered with the SQL engine under the same name but with a `st_` prefix (e.g. `localMin` becomes `st_localMin`).

* `explodeTiles(tileColumn)` - Create a row for each pixel in tile.
* `explodeTileSample(sampleFraction, tileColumn)` - Create a row for each pixel in tile with random sampling. 
* `tileDimensions(tileColumn)` - Query the number of (cols, rows) in a tile.
* `aggHistogram(tileColumn)` - Compute the full column aggregate floating point histogram. 
* `aggStats(tileColumn)` - Compute the full column aggregate floating point statistics. 
* `tileHistogramDouble(tileColumn)` - Compute tileHistogram of floating point tile values.
* `tileStatsDouble(tileColumn)` - Compute statistics of tile values. 
* `tileMeanDouble(tileColumn)` - Compute the tile-wise mean 
* `tileMean(tileColumn)` - Compute the tile-wise mean 
* `tileHistogram(tileColumn)` - Compute tileHistogram of tile values. 
* `tileStats(tileColumn)` - Compute statistics of tile values.
* `localAggStats(tileColumn)` - Compute cell-local aggregate descriptive statistics for a column of tiles.
* `localAggMax(tileColumn)` - Compute the cellwise/local max operation between tiles in a column. 
* `localAggMin(tileColumn)` - Compute the cellwise/local min operation between tiles in a column.
* `localAggMean(tileColumn)` - Compute the cellwise/local mean operation between tiles in a column. 
* `localAggCount(tileColumn)` - Compute the cellwise/local count of non-NoData cells for all tiles in a column. 
* `localAdd(tileColumn)` - Cellwise addition between two tiles. 
* `localSubtract(tileColumn)` - Cellwise subtraction between two tiles. 
* `localAlgebra(tileColumn)` - Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two tile columns. 
* `renderAscii(tileColumn)` - Render tile as ASCII string for debugging purposes. 
