# Creating RasterFrames

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
```

## From `ProjectedExtent`

The simplest mechanism for getting a RasterFrame is to use the `toRF(tileCols, tileRows)` extension method on `ProjectedRaster`. 

```tut
val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
rf.show(5, false)
```

## From `TileLayerRDD`

Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html), to get a `TileLayerRDD` for which there's also a `toRF` extension method. 

```scala
import geotrellis.spark._
val tiledLayer: TileLayerRDD[SpatialKey] = ???
val rf = tiledLayer.toRF
```

Now that we have a `RasterFrame` to work with, let's explore in the next section what we can do with it.

```tut:invisible
spark.stop()
```
