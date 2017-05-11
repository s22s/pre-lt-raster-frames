package org.apache.spark.sql.gt.types

import geotrellis.raster.{MultibandTile, Tile}

/**
 * UDT for singleband tiles.
 * @author sfitch 
 * @since 5/11/17
 */
private[gt] class TileUDT extends AbstractTileUDT[Tile]("st_tile")
case object TileUDT extends TileUDT

/**
 * UDT for multiband tiles.
 * @author sfitch
 * @since 5/11/17
 */
private[gt] class MultibandTileUDT extends AbstractTileUDT[MultibandTile]("st_multibandtile")
case object MultibandTileUDT extends MultibandTileUDT
