name := "raster-frames-datasource"

libraryDependencies ++= Seq(
  geotrellis("s3").value
)

// Run generateDocs to help convert examples to tut docs.
docsMap := Map(baseDirectory.value / "src" / "test" -> target.value / "literator" )
