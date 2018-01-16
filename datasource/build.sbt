name := "raster-frames-datasource"

libraryDependencies ++= Seq(
  geotrellis("s3") % Provided
)

fork in Test := false
