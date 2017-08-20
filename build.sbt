
import sbt._
import sbt.Keys._

lazy val root = project
  .in(file("."))
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings: _*)
  .settings(docsSettings: _*)
  .enablePlugins(MicrositesPlugin)


