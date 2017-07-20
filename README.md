# RasterFrames

_RasterFrames_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of [GeoTrellis](https://geotrellis.io/).

Here are some examples on how to use it.

## Setup

0\. sbt configuration

```scala
// TODO
```

1\. First, apply `import`s, initialize the `SparkSession`, and initialize RasterFrames with Spark:  
```scala
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

```scala
val scene = spark.sparkContext.hadoopGeoTiffRDD("src/test/resources/L8-B8-Robinson-IL.tiff")
val layout = FloatingLayoutScheme(10, 10)
val layerMetadata = TileLayerMetadata.fromRdd(scene, LatLng, layout)._2
val tiled: TileLayerRDD[SpatialKey] = ContextRDD(scene.tileToLayout(layerMetadata), layerMetadata)
```

3\. The `astraea.spark.rasterframes._` import adds the `.toRF` extension method to `TileLayerRDD`.
```scala
scala> val rf: RasterFrame = tiled.toRF
rf: astraea.spark.rasterframes.RasterFrame = [key: struct<col: int, row: int>, tile: st_tile]

scala> rf.show(5, false)
+-------+--------------------------------------------------------+
|key    |tile                                                    |
+-------+--------------------------------------------------------+
|[40,13]|geotrellis.raster.UShortConstantNoDataArrayTile@7bfca44c|
|[19,37]|geotrellis.raster.UShortConstantNoDataArrayTile@5ee61078|
|[10,38]|geotrellis.raster.UShortConstantNoDataArrayTile@24a268d1|
|[43,23]|geotrellis.raster.UShortConstantNoDataArrayTile@3a51694b|
|[3,1]  |geotrellis.raster.UShortConstantNoDataArrayTile@24116489|
+-------+--------------------------------------------------------+
only showing top 5 rows

```

4\. Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

## Inspection
```scala
scala> rf.tileColumns
res4: Seq[String] = ArraySeq(tile)

scala> rf.spatialKeyColumn
res5: String = key
```

## Tile Statistics 
```scala
scala> rf.select(tileDimensions($"tile")).show(5)
+--------------------+
|tileDimensions(tile)|
+--------------------+
|             [10,10]|
|             [10,10]|
|             [10,10]|
|             [10,10]|
|             [10,10]|
+--------------------+
only showing top 5 rows


scala> rf.select(tileMean($"tile")).show(5)
+------------------+
|    tileMean(tile)|
+------------------+
| 9890.929999999997|
|10691.170000000002|
| 7832.290000000001|
|11429.979999999996|
| 8726.010000000007|
+------------------+
only showing top 5 rows


scala> rf.select(tileStats($"tile")).show(5)
+---------+------------------+------+-----+------------------+----+-----+
|dataCells|              mean|median| mode|            stddev|zmin| zmax|
+---------+------------------+------+-----+------------------+----+-----+
|      100| 9890.929999999997|  9021| 8016| 1939.854619578488|7930|13766|
|      100|10691.170000000002| 10703|10695|152.81616766559748|9999|11071|
|      100| 7832.290000000001|  7753| 7734|329.10388314330174|7586| 9930|
|      100|11429.979999999996| 10876| 8396| 2897.988108947309|7976|20848|
|      100| 8726.010000000007|  8649| 8421|411.32166232767264|7903| 9733|
+---------+------------------+------+-----+------------------+----+-----+
only showing top 5 rows


scala> // Extract quantile breaks from histogram computation
     | rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
+-----------------------------------+
|value                              |
+-----------------------------------+
|[8090, 8191, 10424, 12081, 13766]  |
|[10587, 10679, 10729, 10816, 11071]|
|[7695, 7731, 7761, 7793, 9930]     |
|[8674, 10026, 11325, 13664, 20848] |
|[8403, 8595, 8715, 9095, 9733]     |
+-----------------------------------+
only showing top 5 rows

```

## Aggregate Statistics

```scala
scala> rf.select(aggStats($"tile")).show()
+---------+------------------+-----------------+------------------+------------------+-----------------+-------+
|dataCells|              mean|           median|              mode|            stddev|             zmin|   zmax|
+---------+------------------+-----------------+------------------+------------------+-----------------+-------+
|   384421|10165.074296149278|9899.587602968166|11247.958326988448|1818.5590590509848|7502.598896044158|39158.0|
+---------+------------------+-----------------+------------------+------------------+-----------------+-------+


scala> // Extract bin counts from bins
     | rf.select(aggHistogram($"tile")).
     |   map(h => for(v <- h.values) yield(v, h.itemCount(v))).
     |   select(explode($"value") as "counts").
     |   select("counts._1", "counts._2").
     |   toDF("value", "count").
     |   orderBy(desc("count")).
     |   show(5)
+------------------+-----+
|             value|count|
+------------------+-----+
| 7502.598896044158|  100|
|10359.457614971237|   92|
|10760.714442208247|   90|
|11247.958326988448|   85|
| 8108.176962676963|   81|
+------------------+-----+
only showing top 5 rows

```

## Arbitrary GeoTrellis Operations

```scala
scala> import geotrellis.raster.equalization._
import geotrellis.raster.equalization._

scala> val equalizer = udf((t: Tile) => t.equalize())
equalizer: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,org.apache.spark.sql.gt.types.TileUDT@30347142,Some(List(org.apache.spark.sql.gt.types.TileUDT@30347142)))

scala> rf.select(tileMean(equalizer($"tile")) as "equalizedMean").show(5, false)
+------------------+
|equalizedMean     |
+------------------+
|33138.121212121216|
|33198.30303030304 |
|33238.42424242424 |
|33111.37373737374 |
|33124.74747474747 |
+------------------+
only showing top 5 rows


scala> val downsample = udf((t: Tile) => t.resample(4, 4))
downsample: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,org.apache.spark.sql.gt.types.TileUDT@30347142,Some(List(org.apache.spark.sql.gt.types.TileUDT@30347142)))

scala> rf.select(renderAscii(downsample($"tile") as "minime")).show(5, false)
+-----------------------------------------------------------------------------------------------------+
|renderAscii(alias)                                                                                   |
+-----------------------------------------------------------------------------------------------------+
| 11218 12580 12183 11717
 11207 12357 12412 12735
  8275  8098  8015  8099
  8043  8035  8143  8163

|
| 10668 10707 10403 10421
 10778 10634 10868 10755
 10760 10758 10725 10529
 10870 10729 10651 10545

|
|  7730  7736  7696  7801
  7761  7762  7672  7730
  7734  7633  7775  7768
  7770  7793  9930  7709

|
| 12273 10686 14906 13276
  9209  9149 18211 19635
  8876  8202  9653 10219
  8942 10277 12429 13860

|
|  9518  9733  9423  9501
  8265  8710  8595  8649
  9292  8102  8421  8448
  8843  7991  9170  9102

|
+-----------------------------------------------------------------------------------------------------+
only showing top 5 rows

```

## Basic Interop with SparkML

```scala
scala> import org.apache.spark.ml._
import org.apache.spark.ml._

scala> import org.apache.spark.ml.feature._
import org.apache.spark.ml.feature._

scala> val exploded = rf.select($"key", explodeTiles($"tile")).withColumnRenamed("tile", "pixel")
exploded: org.apache.spark.sql.DataFrame = [key: struct<col: int, row: int>, column: int ... 2 more fields]

scala> exploded.printSchema
root
 |-- key: struct (nullable = true)
 |    |-- col: integer (nullable = true)
 |    |-- row: integer (nullable = true)
 |-- column: integer (nullable = false)
 |-- row: integer (nullable = false)
 |-- pixel: double (nullable = false)


scala> val discretizer = new QuantileDiscretizer().
     |   setInputCol("pixel").
     |   setOutputCol("binned").
     |   setHandleInvalid("skip").
     |   setNumBuckets(5)
discretizer: org.apache.spark.ml.feature.QuantileDiscretizer = quantileDiscretizer_94bd6711b9b4

scala> val binned = discretizer.fit(exploded).transform(exploded)
binned: org.apache.spark.sql.DataFrame = [key: struct<col: int, row: int>, column: int ... 3 more fields]

scala> binned.show(false)
+-------+------+---+-------+------+
|key    |column|row|pixel  |binned|
+-------+------+---+-------+------+
|[40,13]|0     |0  |11342.0|3.0   |
|[40,13]|1     |0  |11364.0|3.0   |
|[40,13]|2     |0  |11840.0|4.0   |
|[40,13]|3     |0  |12211.0|4.0   |
|[40,13]|4     |0  |11942.0|4.0   |
|[40,13]|5     |0  |11999.0|4.0   |
|[40,13]|6     |0  |12017.0|4.0   |
|[40,13]|7     |0  |12242.0|4.0   |
|[40,13]|8     |0  |11661.0|4.0   |
|[40,13]|9     |0  |11449.0|3.0   |
|[40,13]|0     |1  |11255.0|3.0   |
|[40,13]|1     |1  |11218.0|3.0   |
|[40,13]|2     |1  |11766.0|4.0   |
|[40,13]|3     |1  |12580.0|4.0   |
|[40,13]|4     |1  |12496.0|4.0   |
|[40,13]|5     |1  |12694.0|4.0   |
|[40,13]|6     |1  |12183.0|4.0   |
|[40,13]|7     |1  |12087.0|4.0   |
|[40,13]|8     |1  |11717.0|4.0   |
|[40,13]|9     |1  |11813.0|4.0   |
+-------+------+---+-------+------+
only showing top 20 rows


scala> binned.groupBy("binned").count().show(false)
+------+-----+
|binned|count|
+------+-----+
|0.0   |77045|
|1.0   |77725|
|4.0   |77701|
|3.0   |76873|
|2.0   |77656|
+------+-----+

```






