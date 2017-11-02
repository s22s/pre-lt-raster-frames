
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys.assembly

lazy val `raster-frames` = project
  .in(file("."))
  .enablePlugins(
    SiteScaladocPlugin,
    ParadoxSitePlugin,
    TutPlugin,
    GhpagesPlugin,
    BuildInfoPlugin,
    AssemblyPlugin
  )
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings)
  .settings(buildInfoSettings)
  .settings(assemblySettings)
  .settings(spSettings)
  .settings(docSettings)

lazy val bench = project
  .dependsOn(`raster-frames`)
  .disablePlugins(
    SparkPackagePlugin, ScoverageSbtPlugin, SitePlugin, BintrayPlugin,
    ReleasePlugin, AssemblyPlugin, SitePreviewPlugin
  )

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
  |    .withRasterFrames
  |spark.sparkContext.setLogLevel("ERROR")
  |import spark.implicits._
  |
""".stripMargin

cleanupCommands in console := """
  |spark.stop()
""".stripMargin
