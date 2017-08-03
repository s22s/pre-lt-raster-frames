package org.apache.spark.sql.gt.types

import geotrellis.raster.{Tile, TileFeature}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.Implicits.singlebandTileEncoder

import scala.reflect.runtime.universe._

/**
 * Encoder for [[TileFeature]].
 *
 * @author sfitch 
 * @since 8/2/17
 */
object TileFeatureEncoder extends DelegatingSubfieldEncoder {
//  def apply[D: TypeTag](): Encoder[TileFeature[Tile, D]] = {
//    create(Seq(
//      "tile" -> singlebandTileEncoder,
//      "data" -> ExpressionEncoder[D]()
//    ))
//  }
}
