package astraea.spark.rasterframes

import geotrellis.util.{LazyLogging, MethodExtensions}
import org.apache.spark.sql.gt.types.TileUDT

/**
 * Extension methods on [[RasterFrame]] type.
 * @author sfitch
 * @since 7/18/17
 */
abstract class RasterFrameMethods extends MethodExtensions[RasterFrame] with LazyLogging {
  /** Get the names of the columns that are of type `Tile` */
  def tileColumns: Seq[String] = self.schema.fields
    .filter(_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
    .map(_.name)

  def spatialColumn: String = ???
  def temporalColumn: Option[String] = ???

  def spatialJoin(right: RasterFrame): RasterFrame = {
    val metadata = self.schema.head.metadata
    // TODO: WARNING this a proper spatial join, but just a placeholder.
    logger.warn("Multi-layer query assumes same tile layout")
    val joined = self.join(right, Seq("key"), "outer")
    joined.setMetadata("key", metadata)
  }
}

