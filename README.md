<img src="src/main/paradox/_template/images/RasterFramesLogo.png" width="300px"/><sup style="vertical-align: top;">&trade;</sup>

[ ![Download](https://api.bintray.com/packages/s22s/maven/raster-frames/images/download.svg) ](https://bintray.com/s22s/maven/raster-frames/_latestVersion) [![Build Status](https://travis-ci.org/s22s/raster-frames.svg?branch=develop)](https://travis-ci.org/s22s/raster-frames) [![codecov](https://codecov.io/gh/s22s/raster-frames/branch/develop/graph/badge.svg)](https://codecov.io/gh/s22s/raster-frames)
 [![Join the chat at https://gitter.im/s22s/raster-frames](https://badges.gitter.im/s22s/raster-frames.svg)](https://gitter.im/s22s/raster-frames?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

_RasterFrames™_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer operations of [GeoTrellis](https://geotrellis.io/).

<img src="src/main/tut/RasterFramePipelineOverview.png" width="600px"/>

> **RasterFrames™ is a new project under active development**. Feedback and contributions are welcomed 
as we continue to improve it.

## Getting Started

RasterFrames™ is currently available for Scala 2.11 + Spark 2.1.0 and is published via Bintray's JCenter, one of the default sbt resolvers. To use, just add the following library dependency:

sbt:

```scala
libraryDependencies += "io.astraea" %% "raster-frames" % "x.y.z"
```

Maven:

```
<dependency>
  <groupId>io.astraea</groupId>
  <artifactId>raster-frames</artifactId>
  <version>x.y.z</version>
</dependency>
```

## Documentation

* [Users' Manual](http://rasterframes.io/)
* [API Documentation](http://rasterframes.io/latest/api/index.html) 
* [List of available UDFs](http://rasterframes.io/reference.html)


## Copyright and License

All code is available to you under the Apache 2.0 License, copyright Astraea, Inc. 2017.


