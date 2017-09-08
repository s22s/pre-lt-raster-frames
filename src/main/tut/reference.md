# Reference

API Scaladoc can be found @ref:[here](latest/api/index.html).

Here's a preliminary list of RasterFrame UDFs available. For the most up to date list, look at the source code for 
@scaladoc[`ColumnFunctions`][ColumnFunctions]. These UDFs are also registered with the SQL engine under the same name 
but with a `st_` prefix (e.g. `localMin` becomes `st_localMin`). 

@@@ note

Please [submit an issue](https://github.com/s22s/raster-frames/issues) if there's a particular function you think should be included.

@@@

* @scaladoc[`explodeTiles`][explodeTiles] - Create a row for each pixel in tile.
* @scaladoc[`explodeTileSample`][explodeTileSample] - Create a row for each pixel in tile with random sampling. 
* @scaladoc[`tileDimensions`][tileDimensions] - Query the number of (cols, rows) in a tile.
* @scaladoc[`aggHistogram`][aggHistogram] - Compute the full column aggregate floating point histogram. 
* @scaladoc[`aggStats`][aggStats] - Compute the full column aggregate floating point statistics. 
* @scaladoc[`tileHistogramDouble`][tileHistogramDouble] - Compute tileHistogram of floating point tile values.
* @scaladoc[`tileStatsDouble`][tileStatsDouble] - Compute statistics of tile values. 
* @scaladoc[`tileMeanDouble`][tileMeanDouble] - Compute the tile-wise mean 
* @scaladoc[`tileMean`][tileMean] - Compute the tile-wise mean 
* @scaladoc[`tileHistogram`][tileHistogram] - Compute tileHistogram of tile values. 
* @scaladoc[`tileStats`][tileStats] - Compute statistics of tile values.
* @scaladoc[`nodataCells`][nodataCells] - Counts the number of `NoData` cells per tile.
* @scaladoc[`dataCells`][dataCells] - Counts the number of non-`NoData` cells per tile.
* @scaladoc[`localAggStats`][localAggStats] - Compute cell-local aggregate descriptive statistics for a column of tiles.
* @scaladoc[`localAggMax`][localAggMax] - Compute the cellwise/local max operation between tiles in a column. 
* @scaladoc[`localAggMin`][localAggMin] - Compute the cellwise/local min operation between tiles in a column.
* @scaladoc[`localAggMean`][localAggMean] - Compute the cellwise/local mean operation between tiles in a column. 
* @scaladoc[`localAggCount`][localAggCount] - Compute the cellwise/local count of non-NoData cells for all tiles in a column. 
* @scaladoc[`localAdd`][localAdd] - Cellwise addition between two tiles. 
* @scaladoc[`localSubtract`][localSubtract] - Cellwise subtraction between two tiles. 
* @scaladoc[`localAlgebra`][localAlgebra] - Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two tile columns. 
* @scaladoc[`renderAscii`][renderAscii] - Render tile as ASCII string for debugging purposes. 

[ColumnFunctions]: astraea.spark.rasterframes.functions.ColumnFunctions
[explodeTiles]: astraea.spark.rasterframes.functions.ColumnFunctions#explodeTiles%28Column*%29:Column
[explodeTileSample]: astraea.spark.rasterframes.functions.ColumnFunctions#explodeTileSample%28Double,Column*%29:Column
[tileDimensions]: astraea.spark.rasterframes.functions.ColumnFunctions#tileDimensions%28Column%29:Column 
<!-- TODO: Fix these -->
[aggHistogram]:  astraea.spark.rasterframes.functions.ColumnFunctions#aggHistogram
[aggStats]: astraea.spark.rasterframes.functions.ColumnFunctions#aggStats
[tileHistogramDouble]: astraea.spark.rasterframes.functions.ColumnFunctions#tileHistogramDouble
[tileStatsDouble]: astraea.spark.rasterframes.functions.ColumnFunctions#tileStatsDouble
[tileMeanDouble]: astraea.spark.rasterframes.functions.ColumnFunctions#tileMeanDouble 
[tileMean]: astraea.spark.rasterframes.functions.ColumnFunctions#tileMean
[tileHistogram]: astraea.spark.rasterframes.functions.ColumnFunctions#tileHistogram
[tileStats]: astraea.spark.rasterframes.functions.ColumnFunctions#tileStats
[nodataCells]: astraea.spark.rasterframes.functions.ColumnFunctions#nodataCells
[dataCells]: astraea.spark.rasterframes.functions.ColumnFunctions#dataCells
[localAggStats]: astraea.spark.rasterframes.functions.ColumnFunctions#localAggStats
[localAggMax]: astraea.spark.rasterframes.functions.ColumnFunctions#localAggMax
[localAggMin]: astraea.spark.rasterframes.functions.ColumnFunctions#localAggMin
[localAggMean]: astraea.spark.rasterframes.functions.ColumnFunctions#localAggMean
[localAggCount]: astraea.spark.rasterframes.functions.ColumnFunctions
[localAdd]: astraea.spark.rasterframes.functions.ColumnFunctions#localAdd
[localSubtract]: astraea.spark.rasterframes.functions.ColumnFunctions#localSubtract
[localAlgebra]: astraea.spark.rasterframes.functions.ColumnFunctions#localAlgebra
[renderAscii]: astraea.spark.rasterframes.functions.ColumnFunctions#renderAscii
