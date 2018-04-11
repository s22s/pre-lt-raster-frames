addCommandAlias("makeSite", "docs/makeSite")
addCommandAlias("console", "datasource/console")

lazy val root = project
  .in(file("."))
  .withId("RasterFrames")
  .aggregate(core, datasource, experimental)
  .settings(publishArtifact := false)
  .settings(releaseSettings)

lazy val core = project

lazy val datasource = project
  .dependsOn(core % "test->test;compile->compile")

lazy val experimental = project
  .dependsOn(core % "test->test;compile->compile")
  .dependsOn(datasource % "test->test;compile->compile")


lazy val docs = project
  .dependsOn(core, datasource)

lazy val bench = project
  .dependsOn(core)


