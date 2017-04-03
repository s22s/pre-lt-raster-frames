/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.sql

import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Spark UDTs for Geotrellis
 *
 * @author sfitch 
 * @since 4/3/17
 */
private[spark] object GTSQLTypes {
  def register(sqlContext: SQLContext): Unit = {
    register(MultibandTileUDT)
    register(TileUDT)
  }

  private[spark] def register(udt: GeoTrellisUDT[_]): Unit = {
    UDTRegistration.register(
      udt.userClass.getCanonicalName,
      udt.getClass.getSuperclass.getName
    )
  }

  def runtimeClass[T: TypeTag]: Class[T] =
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  private[spark] abstract class GeoTrellisUDT[T >: Null: AvroRecordCodec: TypeTag](override val simpleString: String)
    extends UserDefinedType[T] {
    override def sqlType: StructType = StructType(Array(StructField(simpleString, BinaryType)))
    override def serialize(obj: T): InternalRow = {
      val bytes = AvroEncoder.toBinary[T](obj)
      new GenericInternalRow(Array[Any](bytes))
    }

    override def deserialize(datum: Any): T = {
      val row = datum.asInstanceOf[InternalRow]
      AvroEncoder.fromBinary[T](row.getBinary(0))
    }

    override def userClass: Class[T] = runtimeClass[T]
  }

  private [spark] class MultibandTileUDT extends GeoTrellisUDT[MultibandTile]("st_multibandtile")
  object MultibandTileUDT extends MultibandTileUDT

  private [spark] class TileUDT extends GeoTrellisUDT[Tile]("st_tile")
  object TileUDT extends TileUDT

}
