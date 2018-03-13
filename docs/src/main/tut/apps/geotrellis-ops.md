# GeoTrellis Operations

```tut:invisible
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().
  master("local[*]").appName("RasterFrames").getOrCreate().withRasterFrames
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
val scene = SinglebandGeoTiff("../core/src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
```


GeoTrellis provides a [rich set of Map Algebra operations](https://docs.geotrellis.io/en/latest/guide/core-concepts.html#map-algebra) and other tile processing features that can be used with RasterFrames via Spark's UDF support.

Here's an example creating a UDFs to invoke the `equalize` transformation on each tile in a RasterFrame, and then compute the resulting per-tile mean of it.

```tut
import geotrellis.raster.equalization._
val equalizer = udf((t: Tile) => t.equalize())
val equalized = rf.select(equalizer($"tile") as "equalized")
equalized.select(tileMean($"equalized") as "equalizedMean").show(5, false)
```

Here's an example downsampling a tile and rendering each tile as a matrix of numerical values.

```tut  
val downsample = udf((t: Tile) => t.resample(4, 4))
val downsampled = rf.select(renderAscii(downsample($"tile")) as "minime")
downsampled.show(5, false)
```


```tut:invisible
spark.stop()
```

