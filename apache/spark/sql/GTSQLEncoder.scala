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

import geotrellis.raster.Tile
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.spark.sql.GTSQLTypes.{GeoTrellisUDT, TileUDT}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
/**
 *
 * @author sfitch 
 * @since 4/3/17
 */
object GTSQLEncoder {
  def apply[T >: Null: AvroRecordCodec: TypeTag](udt: GeoTrellisUDT[T]): Encoder[T] = {
    val ct =  ClassTag[T](typeTag[T].mirror.runtimeClass( typeTag[T].tpe ))
    val inputObject = BoundReference(0, udt, nullable = false)
    val inputRow = GetColumnByOrdinal(0, udt)
    ExpressionEncoder[T](
      udt.sqlType,
      false,
      Seq(UDTSerializer(udt, inputObject)),
      UDTDeserializer(udt, inputRow),
      ct
    )
  }

  case class UDTSerializer[T >: Null: AvroRecordCodec] private[spark] (
    udt: GeoTrellisUDT[T], child: Expression)
    extends UnaryExpression with CodegenFallback {

    override def dataType: DataType = udt.sqlType

    override protected def nullSafeEval(input: Any): InternalRow = {
      val value = input.asInstanceOf[T]
      udt.serialize(value)
    }

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil
  }

  case class UDTDeserializer[T >: Null: AvroRecordCodec: TypeTag] private[spark] (
    udt: GeoTrellisUDT[T], child: Expression)
    extends UnaryExpression with CodegenFallback {

    override def dataType: DataType = ScalaReflection.dataTypeFor[T]

    override protected def nullSafeEval(input: Any): Any  = {
      val row = input.asInstanceOf[InternalRow]
      udt.deserialize(row)
    }
    override protected def otherCopyArgs: Seq[AnyRef] =
      implicitly[AvroRecordCodec[T]] :: typeTag[T] :: Nil
  }
}
