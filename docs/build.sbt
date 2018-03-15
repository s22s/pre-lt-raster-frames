import com.typesafe.sbt.SbtGit.git

enablePlugins(SiteScaladocPlugin, ParadoxSitePlugin, TutPlugin, GhpagesPlugin, ScalaUnidocPlugin)

name := "raster-frames-docs"

libraryDependencies ++= Seq(
  spark("mllib").value % Tut,
  spark("sql").value % Tut
)

git.remoteRepo := "git@github.com:s22s/raster-frames.git"
apiURL := Some(url("http://rasterframes.io/latest/api"))
autoAPIMappings := true
ghpagesNoJekyll := true

ScalaUnidoc / siteSubdirName := "latest/api"

addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName)

Paradox / paradoxProperties ++= Map(
  "github.base_url" -> "https://github.com/s22s/raster-frames",
  //"scaladoc.org.apache.spark.sql.gt" -> "http://rasterframes.io/latest",
  //"scaladoc.geotrellis.base_url" -> "https://geotrellis.github.io/scaladocs/latest",
  "snip.pyexamples.base_dir" -> (baseDirectory.value + "/../pyrasterframes/python/test/examples")
)
Paradox / paradoxTheme := Some(builtinParadoxTheme("generic"))
Paradox / paradoxGroups := Map("Language" -> Seq("Scala", "Python"))
Paradox / paradoxTheme / sourceDirectory := sourceDirectory.value / "main" / "paradox" / "_template"

Compile / doc / scalacOptions++= Seq( "-J-Xmx6G", "-no-link-warnings")

Tut / run / fork := true

Tut / run / javaOptions := Seq("-Xmx8G", "-Dspark.ui.enabled=false")

val skipTut = false

if (skipTut) Seq(
  Paradox / sourceDirectory := tutSourceDirectory.value,
  makeSite := makeSite.dependsOn(Compile / unidoc).value
)
else Seq(
  Paradox / sourceDirectory := tutTargetDirectory.value,
  makeSite := makeSite.dependsOn(tutQuick, Compile / unidoc).value
)
