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

```scala
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

```scala
scala> val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
scene: geotrellis.raster.io.geotiff.SinglebandGeoTiff = SinglebandGeoTiff(geotrellis.raster.UShortConstantNoDataArrayTile@402bd015,Extent(431902.5, 4313647.5, 443512.5, 4321147.5),geotrellis.proj4.CRS$$anon$3@87f96f9f,Tags(Map(AREA_OR_POINT -> POINT),List(Map())),GeoTiffOptions(geotrellis.raster.io.geotiff.Striped@737cbac8,geotrellis.raster.io.geotiff.compression.DeflateCompression$@6dc977ca,1,None))

scala> val rf = scene.projectedRaster.toRF(128, 128)
rf: astraea.spark.rasterframes.RasterFrame = [key: struct<col: int, row: int>, tile: st_tile]

scala> rf.show(5, false)
+-----+--------------------------------------------------------+
|key  |tile                                                    |
+-----+--------------------------------------------------------+
|[0,0]|geotrellis.raster.UShortConstantNoDataArrayTile@12e06760|
|[1,1]|geotrellis.raster.UShortConstantNoDataArrayTile@409c3d49|
|[6,1]|geotrellis.raster.UShortConstantNoDataArrayTile@39287485|
|[3,1]|geotrellis.raster.UShortConstantNoDataArrayTile@205299ee|
|[4,2]|geotrellis.raster.UShortConstantNoDataArrayTile@319821e3|
+-----+--------------------------------------------------------+
only showing top 5 rows

```

## Raster Analysis

Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

### Inspection

```scala
scala> rf.tileColumns
res4: Seq[String] = ArraySeq(tile)

scala> rf.spatialKeyColumn
res5: String = key
```

### Tile Statistics 

```scala
scala> rf.select(tileDimensions($"tile")).show(5)
+--------------------+
|tileDimensions(tile)|
+--------------------+
|           [128,128]|
|           [128,128]|
|           [128,128]|
|           [128,128]|
|           [128,128]|
+--------------------+
only showing top 5 rows


scala> rf.select(tileMean($"tile")).show(5)
+------------------+
|    tileMean(tile)|
+------------------+
| 10338.11999511721|
|10194.718322753917|
|10729.164062499996|
| 9544.470825195294|
|10782.311645507827|
+------------------+
only showing top 5 rows


scala> rf.select(tileStats($"tile")).show(5)
+---------+------------------+------+-----+------------------+----+-----+
|dataCells|              mean|median| mode|            stddev|zmin| zmax|
+---------+------------------+------+-----+------------------+----+-----+
|    16384| 10338.11999511721| 10685| 7737|1840.2361521578157|7291|23077|
|    16384|10194.718322753917|  9919| 9782|2005.9484818489861|7412|28869|
|      768|10729.164062499996| 11094| 9663|   861.51768587969|7952|12282|
|    16384| 9544.470825195294|  8850| 7781|  2050.22247610873|7470|29722|
|    16384|10782.311645507827| 10990|11026|1473.6688528093296|7485|23590|
+---------+------------------+------+-----+------------------+----+-----+
only showing top 5 rows


scala> // Extract quantile breaks from histogram computation
     | rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
+----------------------------------+
|value                             |
+----------------------------------+
|[8155, 9931, 11126, 12042, 23077] |
|[8670, 9524, 10303, 11212, 28869] |
|[9708, 10744, 11235, 11455, 12282]|
|[7987, 8503, 9260, 10875, 29722]  |
|[9661, 10629, 11293, 11997, 23590]|
+----------------------------------+
only showing top 5 rows

```

### Aggregate Statistics

```scala
scala> rf.select(aggStats($"tile")).show()
+---------+------------------+-----------------+------------------+------------------+-----------------+-------+
|dataCells|              mean|           median|              mode|            stddev|             zmin|   zmax|
+---------+------------------+-----------------+------------------+------------------+-----------------+-------+
|   386941|10160.503048268341|9900.027479106197|10389.873462922193|1817.3730199562644|7397.635610766046|39158.0|
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
| 9997.062125524271|   96|
| 7397.635610766046|   94|
|10769.854489164087|   92|
| 8078.005156192453|   87|
| 9267.153062053289|   83|
+------------------+-----+
only showing top 5 rows

```

### Arbitrary GeoTrellis Operations

```scala
scala> import geotrellis.raster.equalization._
import geotrellis.raster.equalization._

scala> val equalizer = udf((t: Tile) => t.equalize())
equalizer: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,org.apache.spark.sql.gt.types.TileUDT@7a00d81d,Some(List(org.apache.spark.sql.gt.types.TileUDT@7a00d81d)))

scala> val equalized = rf.select(equalizer($"tile") as "equalized")
equalized: org.apache.spark.sql.DataFrame = [equalized: st_tile]

scala> equalized.select(tileMean($"equalized") as "equalizedMean").show(5, false)
+------------------+
|equalizedMean     |
+------------------+
|32776.88976377951 |
|32775.50851492398 |
|30774.931392296898|
|32777.86241836048 |
|32775.866507965664|
+------------------+
only showing top 5 rows


scala> val downsample = udf((t: Tile) => t.resample(4, 4))
downsample: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,org.apache.spark.sql.gt.types.TileUDT@7a00d81d,Some(List(org.apache.spark.sql.gt.types.TileUDT@7a00d81d)))

scala> val downsampled = rf.select(renderAscii(downsample($"tile")) as "minime")
downsampled: org.apache.spark.sql.DataFrame = [minime: string]

scala> downsampled.show(5, false)
+-----------------------------------------------------------------------------------------------------+
|minime                                                                                               |
+-----------------------------------------------------------------------------------------------------+
|  8837  8316 10855 10780
  8125 10082  9930  8290
  7624 10739 12019 11287
 11738  8125 11830 11260

|
| 11278  8090  9976 10251
  9152  9952  8598  7867
 11155 10279  9599  7892
 16544 12812  9280 12004

|
|    ND    ND    ND    ND
    ND    ND    ND    ND
    ND    ND    ND    ND
    ND    ND    ND    ND

|
|  8116  7766 10988 11587
  8844  8386  8538  7900
  8219  8007  8739  8615
 10438 13469  8790 13562

|
|  8149  7967 16175 11629
  8453  8974 10055 10609
 12485 12847 11454  9899
  7913 10969 11147  9728

|
+-----------------------------------------------------------------------------------------------------+
only showing top 5 rows

```

### Reassembling Rasters

For the purposes of debugging, the RasterFrame tiles can be reassembled back into a raster for viewing. However, keep in mind that this will download all the data to the driver, and reassemble it in-memory. So it's not appropriate for very large coverages.

```scala
val image = rf.toRaster($"tile", 774, 500)
val colors = ColorMap.fromQuantileBreaks(image.tile.histogram, ColorRamps.BlueToOrange)
image.tile.color(colors).renderPng().write("src/main/tut/raster.png")
```

![](src/main/tut/raster.png)

### Basic Interop with SparkML

```scala
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

```scala
scala> binned.show(false)
+-----+------+---+-------+------+
|key  |column|row|pixel  |binned|
+-----+------+---+-------+------+
|[0,0]|0     |0  |14294.0|4.0   |
|[0,0]|1     |0  |14277.0|4.0   |
|[0,0]|2     |0  |13939.0|4.0   |
|[0,0]|3     |0  |13604.0|4.0   |
|[0,0]|4     |0  |14182.0|4.0   |
|[0,0]|5     |0  |14851.0|4.0   |
|[0,0]|6     |0  |15584.0|4.0   |
|[0,0]|7     |0  |13905.0|4.0   |
|[0,0]|8     |0  |10834.0|3.0   |
|[0,0]|9     |0  |10284.0|2.0   |
|[0,0]|10    |0  |8973.0 |1.0   |
|[0,0]|11    |0  |8314.0 |0.0   |
|[0,0]|12    |0  |8051.0 |0.0   |
|[0,0]|13    |0  |8242.0 |0.0   |
|[0,0]|14    |0  |8448.0 |1.0   |
|[0,0]|15    |0  |8002.0 |0.0   |
|[0,0]|16    |0  |8302.0 |0.0   |
|[0,0]|17    |0  |8419.0 |1.0   |
|[0,0]|18    |0  |8044.0 |0.0   |
|[0,0]|19    |0  |8362.0 |0.0   |
+-----+------+---+-------+------+
only showing top 20 rows


scala> binned.groupBy("binned").count().show(false)
+------+-----+
|binned|count|
+------+-----+
|0.0   |77442|
|1.0   |77246|
|4.0   |77473|
|3.0   |77019|
|2.0   |77820|
+------+-----+

```





