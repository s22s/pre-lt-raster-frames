/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
