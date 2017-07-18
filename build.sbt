import de.heikoseeberger.sbtheader.CommentStyleMapping
import de.heikoseeberger.sbtheader.license.Apache2_0

scalaVersion := "2.11.8"

name := "raster-frames"

organization := "astraea"

version := "0.3.0-SNAPSHOT"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

headers := CommentStyleMapping.createFrom(Apache2_0, "2017", "Astraea, Inc.")

resolvers ++= Seq(
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases"
)

def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.1.0"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.1.0" % "provided"

libraryDependencies ++= Seq(
  geotrellis("spark") % "provided",
  geotrellis("spark-testkit") % Test,
  spark("core"),
  spark("mllib"),
  spark("sql"),
  "org.scalatest" %% "scalatest" % "3.0.3" % Test
)

fork in Test := true

scalacOptions ++= Seq("-feature", "-deprecation")

bintrayOrganization := Some("s22s")

bintrayReleaseOnPublish in ThisBuild := false

publishArtifact in (Compile, packageDoc) := false
