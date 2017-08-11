package astraea.spark.rasterframes

import geotrellis.raster.{ProjectedRaster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io._
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.SparkSession

/**
 * Extension methods on [[ProjectedRaster]] for creating [[RasterFrame]]s.
 *
 * @author sfitch 
 * @since 8/10/17
 */
trait ProjectedRasterMethods extends MethodExtensions[ProjectedRaster[Tile]] {

  /**
   * Convert the wrapped [[ProjectedRaster]] into a [[RasterFrame]] with a
   * single row.
   *
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(implicit spark: SparkSession): RasterFrame = {
    val (cols, rows) = self.raster.dimensions
    toRF(cols, rows)
  }

  /**
   * Convert the wrapped [[ProjectedRaster]] into a [[RasterFrame]] using the
   * given dimensions as the target per-row tile size.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile.
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(tileCols: Int, tileRows: Int)
    (implicit spark: SparkSession): RasterFrame = {

    val layout = LayoutDefinition(self.rasterExtent, tileCols, tileRows)
    val kb = KeyBounds(SpatialKey(0, 0), SpatialKey(layout.layoutCols, layout.layoutRows))
    val tlm = TileLayerMetadata(
      self.tile.cellType, layout, self.extent, self.crs, kb)

    val rdd = spark.sparkContext.makeRDD(Seq((self.projectedExtent, self.tile)))

    val tiled = rdd.tileToLayout(tlm)

    WithContextRDDMethods(ContextRDD(tiled, tlm)).toRF
  }
}
