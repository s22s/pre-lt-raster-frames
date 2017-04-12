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
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, StructField, StructType, UserDefinedType}

import scala.reflect.runtime.universe._


/**
 * Base class for several of the tile-related GeoTrellis UDTs.
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] abstract class GeoTrellisUDT[T >: Null: AvroRecordCodec: TypeTag]
  (override val typeName: String) extends UserDefinedType[T] {

  override val simpleString = typeName

  // TODO: Consider using built-in kryo encoder
  override def sqlType: StructType = StructType(Array(
    StructField(simpleString + "_avro", BinaryType)
  ))

  override def serialize(obj: T): InternalRow = {
    val bytes = AvroEncoder.toBinary[T](obj)
    InternalRow(bytes)
  }

  override def deserialize(datum: Any): T = {
    val row = datum.asInstanceOf[InternalRow]
    AvroEncoder.fromBinary[T](row.getBinary(0))
  }

  override def userClass: Class[T] = runtimeClass[T]
}


private[gt] class MultibandTileUDT extends GeoTrellisUDT[MultibandTile]("st_multibandtile")
object MultibandTileUDT extends MultibandTileUDT

private[gt] class TileUDT extends GeoTrellisUDT[Tile]("st_tile")
object TileUDT extends TileUDT
