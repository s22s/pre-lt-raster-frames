# RasterFrames

[ ![Download](https://api.bintray.com/packages/s22s/maven/raster-frames/images/download.svg) ](https://bintray.com/s22s/maven/raster-frames/_latestVersion) [![Build Status](https://travis-ci.org/s22s/raster-frames.svg?branch=develop)](https://travis-ci.org/s22s/raster-frames)

_RasterFrames_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of [GeoTrellis](https://geotrellis.io/).

Here are some examples on how to use it.

## Setup

### `sbt` configuration

```scala
resolvers += Resolver.bintrayRepo("s22s", "maven")
libraryDependencies += "io.astraea" %% "raster-frames" % "{version}"
```

### Imports and Spark Session Initialization

First, apply `import`s, initialize the `SparkSession`, and initialize RasterFrames with Spark:

```tut:silent
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().master("local").appName("RasterFrames").getOrCreate()

rfInit(spark.sqlContext)
import spark.implicits._
```

## Creating a RasterFrame

The simplest mechanism for getting a RasterFrame is to use the `toRF(tileCols, tileRows)` extension method on `ProjectedRaster`. Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html), to get a `TileLayerRDD` for which there's also a `toRF` extension method. 

```tut
val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128)
rf.show(5, false)
```

## Raster Analysis

Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

### Inspection

```tut
rf.tileColumns
rf.spatialKeyColumn
```

### Tile Statistics 

```tut
rf.select(tileDimensions($"tile")).show(5)
rf.select(tileMean($"tile")).show(5)
rf.select(tileStats($"tile")).show(5)
// Extract quantile breaks from histogram computation
rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
```

### Aggregate Statistics

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

### Arbitrary GeoTrellis Operations

```tut
import geotrellis.raster.equalization._
val equalizer = udf((t: Tile) => t.equalize())
val equalized = rf.select(equalizer($"tile") as "equalized")
equalized.select(tileMean($"equalized") as "equalizedMean").show(5, false)
  
val downsample = udf((t: Tile) => t.resample(4, 4))
val downsampled = rf.select(renderAscii(downsample($"tile")) as "minime")
downsampled.show(5, false)
```

### Reassembling Rasters

For the purposes of debugging, the RasterFrame tiles can be reassembled back into a raster for viewing. However, keep in mind that this will download all the data to the driver, and reassemble it in-memory. So it's not appropriate for very large coverages.

```tut:silent
val image = rf.toRaster($"tile", 774, 500)
val colors = ColorMap.fromQuantileBreaks(image.tile.histogram, ColorRamps.BlueToOrange)
image.tile.color(colors).renderPng().write("src/main/tut/raster.png")
```

![](src/main/tut/raster.png)

### Basic Interop with SparkML

```tut:silent
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
val exploded = rf.select($"key", explodeTiles($"tile")).withColumnRenamed("tile", "pixel")
exploded.printSchema
val discretizer = new QuantileDiscretizer().
  setInputCol("pixel").
  setOutputCol("binned").
  setHandleInvalid("skip").
  setNumBuckets(5)
val binned = discretizer.fit(exploded).transform(exploded)
```

```tut
binned.show(false)
binned.groupBy("binned").count().show(false)
```


```tut:invisible
spark.stop()
```

