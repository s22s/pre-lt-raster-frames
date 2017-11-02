import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.assembly
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import sbtsparkpackage.SparkPackagePlugin
import sbtsparkpackage.SparkPackagePlugin.autoImport._

import _root_.bintray.BintrayPlugin.autoImport._
import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.sbtghpages.GhpagesPlugin
import com.typesafe.sbt.site.SitePlugin.autoImport._
import com.typesafe.sbt.site.paradox.ParadoxSitePlugin.autoImport._
import tut.TutPlugin.autoImport._
import GhpagesPlugin.autoImport._
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._
import sbtassembly.AssemblyPlugin.autoImport._

/**
 * @author sfitch
 * @since 8/20/17
 */
object ProjectPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  val versions = Map(
    "geotrellis" -> "1.2.0-RC1",
    "spark" -> "2.1.1"
  )

  private def geotrellis(module: String) =
    "org.locationtech.geotrellis" %% s"geotrellis-$module" % versions("geotrellis")
  private def spark(module: String) =
    "org.apache.spark" %% s"spark-$module" % versions("spark")

  override def projectSettings = Seq(
    organization := "io.astraea",
    organizationName := "Astraea, Inc.",
    startYear := Some(2017),
    homepage := Some(url("http://rasterframes.io")),
    scmInfo := Some(ScmInfo(url("https://github.com/s22s/raster-frames"), "git@github.com:s22s/raster-frames.git")),
    description := "RasterFrames brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of GeoTrellis",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    scalaVersion := "2.11.11",
    scalacOptions ++= Seq("-feature", "-deprecation"),
    cancelable in Global := true,
    resolvers ++= Seq(
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases"
    ),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.0.0",
      spark("core") % Provided,
      spark("mllib") % Provided,
      spark("sql") % Provided,
      geotrellis("spark"),
      geotrellis("raster"),
      geotrellis("spark-testkit") % Test excludeAll (
        ExclusionRule(organization = "org.scalastic"),
        ExclusionRule(organization = "org.scalatest")
      ),
      "org.scalatest" %% "scalatest" % "3.0.3" % Test
    ),
    excludeDependencies += "com.lightbend.paradox" % "paradox-theme-generic",
    publishArtifact in Test := false,
    fork in Test := true,
    javaOptions in Test := Seq("-Xmx2G"),
    parallelExecution in Test := false,
    developers := List(
      Developer(
        id = "metasim",
        name = "Simeon H.K. Fitch",
        email = "fitch@astraea.io",
        url = url("http://www.astraea.io")
      ),
      Developer(
        id = "mteldridge",
        name = "Matt Eldridge",
        email = "meldridge@astraea.io",
        url = url("http://www.astraea.io")
      )
    )
  )

  object autoImport {


    val skipTut = false

    def docSettings: Seq[Def.Setting[_]] = Seq(
      git.remoteRepo := "git@github.com:s22s/raster-frames.git",
      apiURL := Some(url("http://rasterframes.io/latest/api")),
      autoAPIMappings := false,
      paradoxProperties in Paradox ++= Map(
        "github.base_url" -> "https://github.com/s22s/raster-frames",
        "scaladoc.org.apache.spark.sql.gt" -> "http://rasterframes.io/latest",
        "scaladoc.geotrellis.base_url" -> "https://geotrellis.github.io/scaladocs/latest"
      ),
      paradoxTheme in Paradox := Some(builtinParadoxTheme("generic")),
      sourceDirectory in Paradox in paradoxTheme := sourceDirectory.value / "main" / "paradox" / "_template",
      ghpagesNoJekyll := true,
      scalacOptions in (Compile, doc) ++= Seq(
        "-no-link-warnings"
      ),
      libraryDependencies ++= Seq(
        spark("mllib") % Tut,
        spark("sql") % Tut,
        geotrellis("spark") % Tut,
        geotrellis("raster") % Tut
      ),
      scalacOptions in (Compile, doc) += "-J-Xmx6G"
    ) ++ (
      if (skipTut) Seq(
        sourceDirectory in Paradox := tutSourceDirectory.value
      )
      else Seq(
        sourceDirectory in Paradox := tutTargetDirectory.value,
        makeSite := makeSite.dependsOn(tutQuick).value
      )
    )


    lazy val spJarFile = Def.taskDyn {
      if (spShade.value) {
        Def.task((assembly in spPackage).value)
      } else {
        Def.task(spPackage.value)
      }
    }


    def spSettings: Seq[Def.Setting[_]] = Seq(
      spName := "io.astraea/raster-frames",
      sparkVersion := versions("spark"),
      sparkComponents ++= Seq("sql", "mllib"),
      spAppendScalaVersion := false,
      spIncludeMaven := false,
      spIgnoreProvided := true,
      spShade := true,
      spShortDescription := description.value,
      spHomepage := homepage.value.get.toString,
      spDescription := """
        |RasterFrames brings the power of Spark DataFrames to geospatial raster data,
        |empowered by the map algebra and tile layer operations of GeoTrellis.
        |
        |The underlying purpose of RasterFrames is to allow data scientists and software
        |developers to process and analyze geospatial-temporal raster data with the
        |same flexibility and ease as any other Spark Catalyst data type. At its core
        |is a user-defined type (UDF) called TileUDT, which encodes a GeoTrellis Tile
        |in a form the Spark Catalyst engine can process. Furthermore, we extend the
        |definition of a DataFrame to encompass some additional invariants, allowing
        |for geospatial operations within and between RasterFrames to occur, while
        |still maintaining necessary geo-referencing constructs.
      """.stripMargin,
      test in assembly := {},
      TaskKey[Unit]("pysparkShell") := {
        val jar = spJarFile.value
        println("foo: " + jar)
      }
      //credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
    )

    def releaseSettings: Seq[Def.Setting[_]] = {
      val buildSite: (State) ⇒ State = releaseStepTask(makeSite)
      val publishSite: (State) ⇒ State = releaseStepTask(ghpagesPushSite)
      val releaseArtifacts = releaseStepTask(bintrayRelease)
      Seq(
        bintrayOrganization := Some("s22s"),
        bintrayReleaseOnPublish in ThisBuild := false,
        publishArtifact in (Compile, packageDoc) := false,
        releaseIgnoreUntrackedFiles := true,
        releaseTagName := s"${version.value}",
        releaseProcess := Seq[ReleaseStep](
          checkSnapshotDependencies,
          inquireVersions,
          runClean,
          runTest,
          setReleaseVersion,
          buildSite,
          publishSite,
          commitReleaseVersion,
          tagRelease,
          publishArtifacts,
          releaseArtifacts,
          setNextVersion,
          commitNextVersion,
          pushChanges
        )
      )
    }

  }
}
