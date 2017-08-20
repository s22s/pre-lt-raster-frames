
import sbt._
import sbt.Keys._

lazy val `raster-frames` = project
  .in(file("."))
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings: _*)
  .settings(docSettings: _*)
  .enablePlugins(ParadoxSitePlugin, TutPlugin)
