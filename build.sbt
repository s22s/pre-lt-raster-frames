
import sbt._
import sbt.Keys._

lazy val `raster-frames` = project
  .in(file("."))
  .enablePlugins(SiteScaladocPlugin, ParadoxSitePlugin, TutPlugin, GhpagesPlugin)
  .settings(name := "RasterFrames")
  .settings(moduleName := "raster-frames")
  .settings(releaseSettings: _*)
  .settings(docSettings: _*)
