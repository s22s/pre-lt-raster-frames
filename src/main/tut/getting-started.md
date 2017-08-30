# Getting Started

@@@ note
Most of the examples are shown using the Spark DataFrames API. However, many could also be rewritten to use the Spark SQL API instead. We hope to add more examples in that form in the future.
@@@

## sbt configuration

*RasterFrames* is published via Bintray's JCenter server, which is one of the default sbt resolvers. To use, just add the following library dependency:

```scala
libraryDependencies += "io.astraea" %% "raster-frames" % "{version}"
```

## Initialization

First, some standard `import`s:

```tut:silent
import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
```

Next, initialize the `SparkSession`, and then register RasterFrames with Spark:
 
```tut:silent
implicit val spark = SparkSession.builder().master("local").appName("RasterFrames").getOrCreate()

rfInit(spark.sqlContext)
import spark.implicits._
```


```tut:invisible
spark.stop()
```

