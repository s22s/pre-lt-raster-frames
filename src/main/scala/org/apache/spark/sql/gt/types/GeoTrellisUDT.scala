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
import geotrellis.spark.util.KryoSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, StructField, StructType, UserDefinedType}

import scala.reflect._


/**
 * Base class for several of the tile-related GeoTrellis UDTs.
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] abstract class GeoTrellisUDT[T >: Null: ClassTag]
  (override val typeName: String) extends UserDefinedType[T] {

  override val simpleString = typeName

  override def sqlType: StructType = StructType(Array(
    StructField(simpleString + "_kryo", BinaryType)
  ))

  override def serialize(obj: T): InternalRow = {
    Option(obj)
      .map(KryoSerializer.serialize(_))
      .map(InternalRow.apply(_))
      .orNull
  }

  override def deserialize(datum: Any): T = {
    Option(datum)
      .map(_.asInstanceOf[InternalRow])
      .flatMap(row ⇒ Option(row.getBinary(0)))
      .map(KryoSerializer.deserialize[T])
      .orNull
  }

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}

private[gt] class TileUDT extends GeoTrellisUDT[Tile]("st_tile")
object TileUDT extends TileUDT

private[gt] class MultibandTileUDT extends GeoTrellisUDT[MultibandTile]("st_multibandtile")
object MultibandTileUDT extends MultibandTileUDT

