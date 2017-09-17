# Raster Analysis

```tut:invisible
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark._
import geotrellis.spark.io._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

implicit val spark = SparkSession.builder().master("local").appName("RasterFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rfInit(spark.sqlContext)
import spark.implicits._
val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")
val rf = scene.projectedRaster.toRF(128, 128).cache()
```


Now that we have a `RasterFrame`, we have access to a number of extension methods and columnar functions for performing analysis on tiles.

## Inspection

`RasterFrame` has a number of methods providing access to metadata about the contents of the RasterFrame. 

### Tile Column Names

```tut:book
rf.tileColumns.map(_.toString)
```

### Spatial Key Column Name

```tut:book
rf.spatialKeyColumn.toString
```

### Temporal Key Column

Returns an `Option[Column]` since not all RasterFrames have an explicit temporal dimension.

```tut:book
rf.temporalKeyColumn.map(_.toString)
```

### Tile Layer Metadata

The Tile Layer Metadata defines how the spatial/spatiotemporal domain is discretized into tiles, 
and what the key bounds are.

```tut
import spray.json._
// The `fold` is required because an `Either` is retured, depending on the key type. 
rf.tileLayerMetadata.fold(_.toJson, _.toJson).prettyPrint
```

## Tile Statistics 

### Tile Dimensions

Get the nominal tile dimensions. Depending on the tiling there may be some tiles with different sizes on the edges.

```tut
rf.select(rf.spatialKeyColumn, tileDimensions($"tile")).show(3)
```

### Descriptive Statistics

#### NoData Counts

Count the numer of `NoData` and non-`NoData` cells in each tile.

```tut
rf.select(rf.spatialKeyColumn, nodataCells($"tile"), dataCells($"tile")).show(3)
```

#### Tile Mean

Compute the mean value in each tile. Use `tileMean` for integral cell types, and `tileMeanDouble` for floating point
cell types.
 
```tut
rf.select(rf.spatialKeyColumn, tileMean($"tile")).show(3)
```

#### Tile Summary Statistics

Compute a suite of summary statistics for each tile. Use `tileStats` for integral cells types, and `tileStatsDouble`
for floating point cell types.

```tut
rf.withColumn("stats", tileStats($"tile")).select(rf.spatialKeyColumn, $"stats.*").show(3)
```

### Histogram

The `tileHistogram` function computes a histogram over the data in each tile. See the 
@scaladoc[GeoTrellis `Histogram`](geotrellis.raster.histogram.Histogram) documentation for details on what's
available in the resulting data structure. Use this version for integral cell types, and `tileHistorgramDouble` for
floating  point cells types. 

In this example we compute quantile breaks.

```tut
rf.select(tileHistogram($"tile")).map(_.quantileBreaks(5)).show(5, false)
```

## Aggregate Statistics

The `aggStats` function computes the same summary statistics as `tileStats`, but aggregates them over the whole 
RasterFrame.

```tut
rf.select(aggStats($"tile")).show()
```

A more involved example: extract bin counts from a computed `Histogram`.

```tut
rf.select(aggHistogram($"tile")).
  map(h => for(v <- h.values) yield(v, h.itemCount(v))).
  select(explode($"value") as "counts").
  select("counts._1", "counts._2").
  toDF("value", "count").
  orderBy(desc("count")).
  show(10)
```

## Computing NDVI

Here's an example of computing the 

Normalized Differential Vegetation Index (NDVI) is a standardized vegetation index which allows us to generate an image highlighting differences in relative biomass. 

> “An NDVI is often used worldwide to monitor drought, monitor and predict agricultural production, assist in predicting hazardous fire zones, and map desert encroachment. The NDVI is preferred for global vegetation monitoring because it helps to compensate for changing illumination conditions, surface slope, aspect, and other extraneous factors” (Lillesand. *Remote sensing and image interpretation*. 2004).

```tut:silent
def redBand = SinglebandGeoTiff("src/test/resources/L8-B4-Elkton-VA.tiff").projectedRaster.toRF("red_band")
def nirBand = SinglebandGeoTiff("src/test/resources/L8-B5-Elkton-VA.tiff").projectedRaster.toRF("nir_band")

// Define UDF for computing NDVI from red and NIR bands
val ndvi = udf((red: Tile, nir: Tile) ⇒ {
  val redd = red.convert(DoubleConstantNoDataCellType)
  val nird = nir.convert(DoubleConstantNoDataCellType)
  (nird - redd)/(nird + redd)
})

// We use `asRF` to indicate we know the structure still conforms to RasterFrame constraints
val rf = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asRF

val pr = rf.toRaster($"ndvi", 466, 428)

val brownToGreen = ColorRamp(
  RGBA(166,97,26,255),
  RGBA(223,194,125,255),
  RGBA(245,245,245,255),
  RGBA(128,205,193,255),
  RGBA(1,133,113,255)
).stops(128)

val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
pr.tile.color(colors).renderPng().write("target/scala-2.11/tut/rf-ndvi.png")

//For a georefrenced singleband greyscale image, could do: `GeoTiff(pr).write("ndvi.tiff")`
```

![](rf-ndvi.png)

```tut:invisible
spark.stop()
```

