# Writing to Parquet

It is often useful to write Spark results in a form that is easily reloaded for subsequent analysis. RasterFrames
work just like any other DataFrame in this scenario as long as @scaladoc[`rfInit`][rfInit] is called to register
the imagery types.

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

Let's assume we have a RasterFrame we've done some fancy processing on: 

```tut:silent
import geotrellis.raster.equalization._
val equalizer = udf((t: Tile) => t.equalize())
val equalized = rf.withColumn("equalized", equalizer($"tile"))
```

```tut
equalized.printSchema
equalized.select(aggStats($"tile")).show(false)
equalized.select(aggStats($"equalized")).show(false)
```

We write it out just like any other DataFrame, including the ability to specify partitioning:

```tut:silent
val filePath = "/tmp/equalized.parquet"
equalized.select("*", "spatial_key.*").write.partitionBy("col", "row").mode(SaveMode.Overwrite).parquet(filePath)
```

Let's confirm partitioning happened as expected:

```tut:silent
import java.io.File
new File(filePath).list
```

Now we can load the data back in and check it out:

```tut:silent
val rf2 = spark.read.parquet(filePath)
```

```tut
rf2.printSchema
equalized.select(aggStats($"tile")).show(false)
equalized.select(aggStats($"equalized")).show(false)
```

```tut:invisible
spark.stop()
```

[rfInit]: astraea.spark.rasterframes.package#rfInit%28SQLContext%29:Unit
