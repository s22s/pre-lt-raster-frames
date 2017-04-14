import de.heikoseeberger.sbtheader.CommentStyleMapping
import de.heikoseeberger.sbtheader.license.Apache2_0

scalaVersion := "2.11.8"

name := "geotrellis-spark-sql"

organization := "astraea"

version := "0.1.3-SNAPSHOT"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

headers := CommentStyleMapping.createFrom(Apache2_0, "2017", "Astraea, Inc.")

resolvers ++= Seq(
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases"
)


def geotrellis(module: String) = "org.locationtech.geotrellis" %% s"geotrellis-$module" % "1.1.0-RC1"

def spark(module: String) = "org.apache.spark" %% s"spark-$module" % "2.1.0"

libraryDependencies ++= Seq(
  geotrellis("spark") % "provided",
  geotrellis("spark-testkit") % Test,
  spark("core") % "provided",
  spark("sql") % "provided"
)

scalacOptions += "-feature"

bintrayOrganization := Some("s22s")

bintrayReleaseOnPublish in ThisBuild := false

publishArtifact in (Compile, packageDoc) := false
