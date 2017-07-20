# RasterFrames

_RasterFrames_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of [GeoTrellis](https://geotrellis.io/).

Here are some examples on how to use it.

## Setup

0\. sbt configuration

```scala
// TODO
```

1\. First, apply `import`s, initialize the `SparkSession`, and initialize RasterFrames with Spark:  
```tut:silent
import astraea.spark.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().master("local").appName("RasterFrames").getOrCreate()

rfInit(spark.sqlContext)
import spark.implicits._
```

2\. Next, we go through the steps to create an example GeoTrellis `TileLayerRDD`. Another option is to use a GeoTrellis [`LayerReader`](https://docs.geotrellis.io/en/latest/guide/tile-backends.html). Ther are other mechanisms, but you currently you need a `TileLayerRDD` to create a `RasterFrame`

```tut:silent
val scene = spark.sparkContext.hadoopGeoTiffRDD("src/test/resources/L8-B8-Robinson-IL.tiff")
val layout = FloatingLayoutScheme(10, 10)
val layerMetadata = TileLayerMetadata.fromRdd(scene, LatLng, layout)._2
val tiled: TileLayerRDD[SpatialKey] = ContextRDD(scene.tileToLayout(layerMetadata), layerMetadata)
```

3\. The `astraea.spark.rasterframes._` import adds the `.toRF` extension method to `TileLayerRDD`.
```tut
val rf: RasterFrame = tiled.toRF
rf.show(5, false)
```

4\. Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

## Inspection
```tut
rf.tileColumns
rf.spatialKeyColumn
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

## Arbitrary GeoTrellis Operations

```tut
import geotrellis.raster.equalization._
val equalizer = udf((t: Tile) => t.equalize())
rf.select(tileMean(equalizer($"tile")) as "equalizedMean").show(5, false)
val downsample = udf((t: Tile) => t.resample(4, 4))
rf.select(renderAscii(downsample($"tile") as "minime")).show(5, false)
```

## Basic Interop with SparkML

```tut
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
binned.show(false)
binned.groupBy("binned").count().show(false)
```



```tut:invisible
spark.stop()
```

