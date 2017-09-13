# Raster Analysis

```tut:invisible
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().master("local").appName("RasterFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rfInit(spark.sqlContext)
import spark.implicits._
val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
```


Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

## Inspection

`RasterFrame` has a number of methods providing access to metadata about the contents of the RasterFrame. 

```tut
// Get the names of the columns that are `Tile` columns
rf.tileColumns.map(_.toString)
// Get the column tagged as containing the `SpatialKey`
rf.spatialKeyColumn.toString
```

## Tile Statistics 

```tut
rf.select(tileDimensions($"tile")).show(5)
rf.select(tileMean($"tile")).show(5)
rf.select(tileStats($"tile")).show(5)
// Extract quantile breaks from histogram computation
rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
```

## Aggregate Statistics

```tut
rf.select(aggStats($"tile")).show()
// Extract bin counts from bins
rf.select(aggHistogram($"tile")).
  map(h => for(v <- h.values) yield(v, h.itemCount(v))).
  select(explode($"value") as "counts").
  select("counts._1", "counts._2").
  toDF("value", "count").
  orderBy(desc("count")).
  show(5)
```


```tut:invisible
spark.stop()
```

