package astraea.spark.rasterframes

import geotrellis.util.{LazyLogging, MethodExtensions}
import org.apache.spark.sql.gt.types.TileUDT

/**
 * Extension methods on [[RasterFrame]] type.
 * @author sfitch
 * @since 7/18/17
 */
abstract class RasterFrameMethods extends MethodExtensions[RasterFrame] {
  /** Get the names of the columns that are of type `Tile` */
  def tileColumns: Seq[String] = self.schema.fields
    .filter(_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
    .map(_.name)

  /** Get the spatial column. */
  def spatialKeyColumn: String = {
    val key = self.schema.fields.find(_.metadata.contains("bounds"))
    require(key.nonEmpty, "All RasterFrames must have a column tagged as the spatial key")
    key.get.name
  }

//  def spatialJoin(right: RasterFrame): RasterFrame = {
//    val metadata = self.schema.head.metadata
//    // TODO: WARNING this is not a proper spatial join, but just a placeholder.
//    logger.warn("Multi-layer query assumes same tile layout")
//    val joined = self.join(right, Seq("key"), "outer")
//    joined.setColumnMetadata("key", metadata)
//  }
}

