
import sbt._
import sbt.Keys._

lazy val `raster-frames` = project
  .in(file("."))
  .enablePlugins(SiteScaladocPlugin, ParadoxSitePlugin, ParadoxMaterialThemePlugin, TutPlugin, GhpagesPlugin)
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings: _*)
  .settings(docSettings: _*)

lazy val bench = project
  .dependsOn(`raster-frames`)

initialCommands in console := """
  |import astraea.spark.rasterframes._
  |import geotrellis.raster._
  |import geotrellis.spark.io.kryo.KryoRegistrator
  |import org.apache.spark.serializer.KryoSerializer
  |import org.apache.spark.sql._
  |import org.apache.spark.sql.functions._
  |implicit val spark = SparkSession.builder()
  |    .master("local[*]")
  |    .appName(getClass.getName)
  |    .config("spark.serializer", classOf[KryoSerializer].getName)
  |    .config("spark.kryoserializer.buffer.max", "500m")
  |    .config("spark.kryo.registrationRequired", "false")
  |    .config("spark.kryo.registrator", classOf[KryoRegistrator].getName)
  |    .getOrCreate()
  |spark.sparkContext.setLogLevel("ERROR")
  |rfInit(spark.sqlContext)
  |import spark.implicits._
  |
""".stripMargin

cleanupCommands in console := """
  |spark.stop()
""".stripMargin
