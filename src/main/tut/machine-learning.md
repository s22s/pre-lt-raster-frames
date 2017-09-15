# Machine Learning

@@@ note

This example only scratches the surface of the type of machine learning one can perform with RasterFrames. More
examples are forthcoming. 

@@@

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

Most SparkML algorithms require each observation to be on an independent row. Here's an example of using RasterFrames `explodeTiles` UDFs to do that.

```tut:silent
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
val exploded = rf.select(rf.spatialKeyColumn, explodeTiles($"tile")).withColumnRenamed("tile", "pixel")
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

