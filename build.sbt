lazy val root = project
  .in(file("."))
  .withId("RF")
  .aggregate(core, datasource, pyrasterframes)
  .settings(releaseSettings)

lazy val core = project
  .enablePlugins(
    SiteScaladocPlugin,
    ParadoxSitePlugin,
    TutPlugin,
    GhpagesPlugin,
    BuildInfoPlugin
  )
  .disablePlugins(SparkPackagePlugin)
  .settings(docSettings)

lazy val pyrasterframes = project
  .dependsOn(core, datasource)
  .settings(assemblySettings)

lazy val datasource = project
  .dependsOn(core % "test->test;compile->compile")
  .disablePlugins(SparkPackagePlugin)

lazy val bench = project
  .dependsOn(core)
  .disablePlugins(
    SparkPackagePlugin,
    ScoverageSbtPlugin, SitePlugin,
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
