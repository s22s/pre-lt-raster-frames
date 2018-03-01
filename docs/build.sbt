import com.typesafe.sbt.SbtGit.git

enablePlugins(SiteScaladocPlugin, ParadoxSitePlugin, TutPlugin, GhpagesPlugin)

name := "raster-frames-docs"

libraryDependencies ++= Seq(
  spark("mllib").value % Tut,
  spark("sql").value % Tut
  //,
//  geotrellis("spark").value % Tut,
//  geotrellis("raster").value % Tut
)

git.remoteRepo := "git@github.com:s22s/raster-frames.git"
apiURL := Some(url("http://rasterframes.io/latest/api"))
autoAPIMappings := false
paradoxProperties in Paradox ++= Map(
  "github.base_url" -> "https://github.com/s22s/raster-frames",
  "scaladoc.org.apache.spark.sql.gt" -> "http://rasterframes.io/latest",
  "scaladoc.geotrellis.base_url" -> "https://geotrellis.github.io/scaladocs/latest",
  "snip.pyexamples.base_dir" -> (baseDirectory.value + "/../pyrasterframes/python/test/examples")
)
paradoxTheme in Paradox := Some(builtinParadoxTheme("generic"))
paradoxGroups in Paradox := Map("Language" -> Seq("Scala", "Python"))
sourceDirectory in Paradox in paradoxTheme := sourceDirectory.value / "main" / "paradox" / "_template"
ghpagesNoJekyll := true

scalacOptions in (Compile, doc) ++= Seq( "-J-Xmx6G", "-no-link-warnings")

fork in (Tut, run) := true

javaOptions in (Tut, run) := Seq("-Xmx8G", "-Dspark.ui.enabled=false")

//unmanagedClasspath in Tut ++= (fullClasspath in (LocalProject("datasource"), Compile)).value

val skipTut = false

if (skipTut) Seq(
  sourceDirectory in Paradox := tutSourceDirectory.value
)
else Seq(
  sourceDirectory in Paradox := tutTargetDirectory.value,
  makeSite := makeSite.dependsOn(tutQuick).value
)
