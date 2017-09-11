# RasterFrames

_RasterFrames_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer 
operations of [GeoTrellis](https://geotrellis.io/). The source code can be found on GitHub at
 [s22s/raster-frames](https://github.com/s22s/raster-frames).

@@@ note
RasterFrames is a new project under active development. Feedback and contributions are welcomed as we look to improve it.
@@@

The underlying purpose of RasterFrames is to allow data scientists and software developers the ability to process
and analyze geospatial-temporal raster data with the same flexibility and ease as any other Spark data type. At its
core is a Spark user-defined type (UDF) called `TileUDT`, which encodes a GeoTrellis `Tile` in a form the Spark
Catalyst engine can process. On top of that we extend the definition of a Dataframe to encompass some additional
invariants that allow for geospatial operations within and between RasterFrames to occur while still maintaining 
necessary geo-referencing constructs.

To learn more, please see the [Getting Started](getting-started.md) section of this manual.


@@@ div { .md-left}

## Detailed Contents

@@ toc { depth=2 }

@@@

@@@ div { .md-right }

## Related Links

* [Gitter Channel](https://gitter.im/s22s/raster-frames)
* [API Documentation](latest/api/index.html)
* [GitHub Repository](https://github.com/s22s/raster-frames)
* [GeoTrellis Documentation](https://docs.geotrellis.io/en/latest/)
* [Astraea, Inc.](http://www.astraea.earth/) (the company behind RasterFrames)

@@@

@@@ div { .md-clear }

&nbsp;

@@@

@@@ index
* [Getting Started](getting-started.md)
* [Creating RasterFrames](creating-rasterframes.md)
* [Tile Statistics](tile-statistics.md)
* [Machine Learning](machine-learning.md)
* [GeoTrellis Operations](geotrellis-ops.md)
* [UDF Reference](reference.md)
@@@

