package $package$

import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql.SparkSession

object RasterFramesExample extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .getOrCreate()

  rfInit(spark.sqlContext)

  val scene = SinglebandGeoTiff("src/main/resources/L8-B8-Robinson-IL.tiff")

  val rf = scene.projectedRaster.toRF(128, 128) // <-- tile size

  rf.select(aggMean(rf("tile"))).show(false)

  spark.stop()
}
