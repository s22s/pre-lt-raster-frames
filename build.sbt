
import sbt._
import sbt.Keys._

lazy val `raster-frames` = project
  .in(file("."))
  .enablePlugins(TutPlugin)
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings: _*)
  .settings(docSettings: _*)
