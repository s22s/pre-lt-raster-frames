import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._

import bintray.BintrayPlugin.autoImport._
import com.servicerocket.sbt.release.git.flow.Steps._
import de.heikoseeberger.sbtheader.CommentStyleMapping
import de.heikoseeberger.sbtheader.HeaderKey.headers
import de.heikoseeberger.sbtheader.license.Apache2_0
import tut.TutPlugin.autoImport._

/**
 * @author sfitch
 * @since 8/20/17
 */
object ProjectPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  val versions = Map(
    "geotrellis" -> "1.1.1",
    "spark" -> "2.1.0"
  )

  private def geotrellis(module: String) =
    "org.locationtech.geotrellis" %% s"geotrellis-$module" % versions("geotrellis")
  private def spark(module: String) =
    "org.apache.spark" %% s"spark-$module" % versions("spark")

  override def projectSettings = Seq(
    organization := "io.astraea",
    startYear := Some(2017),
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    headers := CommentStyleMapping.createFrom(Apache2_0, "2017", "Astraea, Inc."),
    scalaVersion := "2.11.11",
    scalacOptions ++= Seq("-feature", "-deprecation"),

    resolvers ++= Seq(
      "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases"
    ),
    libraryDependencies ++= Seq(
      spark("core") % Provided,
      spark("mllib") % Provided,
      spark("mllib") % Tut,
      spark("sql") % Provided,
      spark("sql") % Tut,
      geotrellis("spark") % Provided,
      geotrellis("spark") % Tut,
      geotrellis("raster") % Provided,
      geotrellis("raster") % Tut,
      geotrellis("spark-testkit") % Test excludeAll(
        ExclusionRule(organization = "org.scalastic"),
        ExclusionRule(organization = "org.scalatest")
      ),
      "org.scalatest" %% "scalatest" % "3.0.3" % Test
    ),
    publishArtifact in Test := false,
    fork in Test := true,
    javaOptions in Test := Seq("-Xmx2G"),
    parallelExecution in Test := false
  )

  object autoImport {
    def releaseSettings: Seq[Def.Setting[_]] = {
      val runTut: (State) ⇒ State = releaseStepTask(tut)
      val commitTut = ReleaseStep((st: State) ⇒ {
        val extracted = Project.extract(st)

        val logger = new ProcessLogger {
          def error(s: ⇒ String): Unit = st.log.debug(s)
          def buffer[T](f: ⇒ T): T = f
          def info(s: ⇒ String): Unit = st.log.info(s)
        }

        val vcs = extracted.get(releaseVcs).get
        vcs.add("README.md") ! logger
        val status = vcs.status.!!.trim
        if (status.nonEmpty) {
          vcs.commit("Updated README.md") ! logger
        }
        st
      })
      val releaseArtifacts = releaseStepTask(bintrayRelease)
      Seq(
        bintrayOrganization := Some("s22s"),
        bintrayReleaseOnPublish in ThisBuild := false,
        publishArtifact in (Compile, packageDoc) := false,
        releaseProcess := Seq[ReleaseStep](
          checkSnapshotDependencies,
          checkGitFlowExists,
          inquireVersions,
          runTest,
          gitFlowReleaseStart,
          setReleaseVersion,
          runTut,
          commitTut,
          commitReleaseVersion,
          publishArtifacts,
          releaseArtifacts,
          gitFlowReleaseFinish,
          setNextVersion,
          commitNextVersion
        )
      )
    }

    def docSettings: Seq[Def.Setting[_]] = Seq(
      tutTargetDirectory := baseDirectory.value
    )
  }
}
