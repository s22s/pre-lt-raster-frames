# Getting Started

@@@ note
Most of the examples are shown using the Spark DataFrames API. However, many could also be rewritten to use the Spark SQL API instead. We hope to add more examples in that form in the future.
@@@

## Quick Start

### macOS

1. If not already, install [Homebrew](https://brew.sh/)
2. Run `brew install sbt giter8`
3. Run `g8 git://github.com/s22s/raster-frames`

### Linux

1. Install [sbt](http://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html)
2. Install [giter8](http://www.foundweekends.org/giter8/setup.html)
3. Run `g8 git://github.com/s22s/raster-frames`

### Windows

1. Install [sbt](http://www.scala-sbt.org/release/docs/Installing-sbt-on-Windows.html)
2. Install [giter8](http://www.foundweekends.org/giter8/setup.html)
3. Run `g8 git://github.com/s22s/raster-frames`

## General Setup

*RasterFrames* is published via Bintray's JCenter server, which is one of the default sbt resolvers. To use, just add the following library dependency:

@@dependency[sbt,Maven,Gradle] {
  group="io.astraea"
  artifact="raster-frames_2.11"
  version="x.y.z"
}

