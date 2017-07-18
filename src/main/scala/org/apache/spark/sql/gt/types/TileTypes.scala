package org.apache.spark.sql.gt.types

import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.sql.types.UDTRegistration

/**
 * UDT for singleband tiles.
 * @author sfitch 
 * @since 5/11/17
 */
private[gt] class TileUDT extends AbstractTileUDT[Tile]("st_tile")
case object TileUDT extends TileUDT {
  UDTRegistration.register(classOf[Tile].getName, classOf[TileUDT].getName)
}

/**
 * UDT for multiband tiles.
 * @author sfitch
 * @since 5/11/17
 */
private[gt] class MultibandTileUDT extends AbstractTileUDT[MultibandTile]("st_multibandtile")
case object MultibandTileUDT extends MultibandTileUDT {
  UDTRegistration.register(classOf[MultibandTile].getName, classOf[MultibandTileUDT].getName)
}
