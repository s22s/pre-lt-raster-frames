# RasterFrames

_RasterFrames_ brings the power of Spark DataFrames to geospatial raster data, empowered by the map algebra and tile layer 
operations of [GeoTrellis](https://geotrellis.io/). 

The source code can be found on GitHub at [s22s/raster-frames](https://github.com/s22s/raster-frames).

The underlying purpose of RasterFrames is to allow data scientists and software developers to process
and analyze geospatial-temporal raster data with the same flexibility and ease as any other Spark Catalyst data type. At its
core is a user-defined type (UDF) called @scaladoc[`TileUDT`](org.apache.spark.sql.gt.types.TileUDT), 
which encodes a GeoTrellis @scaladoc[`Tile`](geotrellis.raster.Tile) in a form the Spark Catalyst engine can process. 
Furthermore, we extend the definition of a DataFrame to encompass some additional invariants, allowing for geospatial 
operations within and between RasterFrames to occur, while still maintaining necessary geo-referencing constructs.


![](RasterFrameInvariants.png)

To learn more, please see the [Getting Started](getting-started.md) section of this manual.

@@@ note
RasterFrames is a new project under active development. Feedback and contributions are welcomed as we look to improve it.
@@@

@@@ div { .md-left}

## Detailed Contents

@@ toc { depth=3 }

@@@

@@@ div { .md-right }

## Related Links

* [Gitter Channel](https://gitter.im/s22s/raster-frames)&nbsp;&nbsp;[![Join the chat at https://gitter.im/s22s/raster-frames](https://badges.gitter.im/s22s/raster-frames.svg)](https://gitter.im/s22s/raster-frames?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
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
* [Exporting Rasterframes](exporting-rasterframes.md)
* [Raster Analysis](raster-analysis.md)
* [Machine Learning](machine-learning.md)
* [GeoTrellis Operations](geotrellis-ops.md)
* [UDF Reference](reference.md)
@@@

