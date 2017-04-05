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

import geotrellis.raster.{CellGrid, TileFeature}
import geotrellis.spark.io.avro.AvroRecordCodec
import org.apache.spark.sql.GTSQLTypes.GeoTrellisUDT
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BoundReference, CreateStruct, Expression, GetStructField, If, IsNull, Literal, Or, PrintToStderr, UnaryExpression}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
/**
 *
 * @author sfitch 
 * @since 4/3/17
 */
object GTSQLEncoder {
  def apply[T >: Null: AvroRecordCodec: TypeTag](udt: GeoTrellisUDT[T]): Encoder[T] = {
    val ct = classTagFor[T]

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



  def tileFeatureEncoder[T <: CellGrid: Encoder: TypeTag, D: Encoder: TypeTag]: Encoder[TileFeature[T, D]] = {
    // Much of this is derived from the standard tuple encoder
    val tileEnc = implicitly[Encoder[T]].asInstanceOf[ExpressionEncoder[T]]
    val dataEnc = implicitly[Encoder[D]].asInstanceOf[ExpressionEncoder[D]]

    val ct0 = reflect.classTag[TileFeature[T, D]]
    val cls = Utils.getContextOrSparkClassLoader.loadClass(ct0.runtimeClass.getName)


    val encoders = Seq(tileEnc, dataEnc)
    val fieldNames = Seq("tile", "data")
    encoders.foreach(_.assertUnresolved())

    val schema = StructType(encoders.zip(fieldNames).map {
      case (enc, name) =>
        val (dataType, nullable) = if (enc.flat) {
          enc.schema.head.dataType -> enc.schema.head.nullable
        } else {
          enc.schema -> true
        }
        StructField(s"${name}", dataType, nullable)
    })

    val serializer = encoders.zip(fieldNames).map { case (enc, name) =>
      val originalInputObject = enc.serializer.head.collect { case b: BoundReference => b }.head
      val newInputObject = Invoke(
        BoundReference(0, ObjectType(cls), nullable = false),
        name,
        originalInputObject.dataType)

      val newSerializer = enc.serializer.map(_.transformUp {
        case b: BoundReference if b == originalInputObject => newInputObject
      })

      if (enc.flat) {
        newSerializer.head
      } else {
        val struct =  CreateStruct(newSerializer)
//        val nullCheck = Or(
//          IsNull(newInputObject),
//          Invoke(Literal.fromObject(None), "equals", BooleanType, newInputObject :: Nil))
//        If(nullCheck, Literal.create(null, struct.dataType), struct)
        struct
      }
    }
    val childrenDeserializers = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.deserializer.transform {
          case g: GetColumnByOrdinal => g.copy(ordinal = index)
        }
      } else {
        val input = GetColumnByOrdinal(index, enc.schema)
        val deserialized = enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) =>
            assert(nameParts.length == 1)
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
        }
        If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
      }
    }

    val deserializer =
      NewInstance(cls, childrenDeserializers, ObjectType(cls), propagateNull = false)

    new ExpressionEncoder[TileFeature[T, D]](
      schema,
      flat = false,
      serializer,
      deserializer,
      ClassTag(cls)
    )
  }

  def tfe[T <: CellGrid: Encoder: TypeTag, D: Encoder: TypeTag]: Encoder[TileFeature[T, D]] = {
    val tileEnc = implicitly[Encoder[T]].asInstanceOf[ExpressionEncoder[T]]
    val dataEnc = implicitly[Encoder[D]].asInstanceOf[ExpressionEncoder[D]]
    val encoders = Seq(tileEnc, dataEnc)
    tfe0(encoders).asInstanceOf[Encoder[TileFeature[T, D]]]
  }

  def tfe0(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    encoders.foreach(_.assertUnresolved())

    val fieldNames = Seq("tile", "data")

    val schema = StructType(encoders.zip(fieldNames).map {
      case (e, name) =>
        val (dataType, nullable) = if (e.flat) {
          e.schema.head.dataType -> e.schema.head.nullable
        } else {
          e.schema -> true
        }
        StructField(name, dataType, nullable)
    })

    val cls = Utils.getContextOrSparkClassLoader.loadClass(s"geotrellis.raster.TileFeature")

    val serializer = encoders.zip(fieldNames).map { case (enc, name) =>
      val originalInputObject = enc.serializer.head.collect { case b: BoundReference => b }.head
      val newInputObject = Invoke(
        BoundReference(0, ObjectType(cls), nullable = true),
        name,
        originalInputObject.dataType)

      val newSerializer = enc.serializer.map(_.transformUp {
        case b: BoundReference if b == originalInputObject => newInputObject
      })

      if (enc.flat) {
        newSerializer.head
      } else {
        // For non-flat encoder, the input object is not top level anymore after being combined to
        // a tuple encoder, thus it can be null and we should wrap the `CreateStruct` with `If` and
        // null check to handle null case correctly.
        // e.g. for Encoder[(Int, String)], the serializer expressions will create 2 columns, and is
        // not able to handle the case when the input tuple is null. This is not a problem as there
        // is a check to make sure the input object won't be null. However, if this encoder is used
        // to create a bigger tuple encoder, the original input object becomes a filed of the new
        // input tuple and can be null. So instead of creating a struct directly here, we should add
        // a null/None check and return a null struct if the null/None check fails.
        val struct = CreateStruct(newSerializer)
        val nullCheck = Or(
          IsNull(newInputObject),
          Invoke(Literal.fromObject(None), "equals", BooleanType, newInputObject :: Nil))
        If(nullCheck, Literal.create(null, struct.dataType), struct)
      }
    }

    val childrenDeserializers = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.deserializer.transform {
          case g: GetColumnByOrdinal => g.copy(ordinal = index)
        }
      } else {
        val input = GetColumnByOrdinal(index, enc.schema)
        val deserialized = enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) =>
            assert(nameParts.length == 1)
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
        }
        If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
      }
    }

    val deserializer =
      NewInstance(cls, childrenDeserializers, ObjectType(cls), propagateNull = false)

    new ExpressionEncoder[Any](
      schema,
      flat = false,
      serializer,
      deserializer,
      ClassTag(cls))
  }

  /** Utility to build a ClassTag from a TypeTag. */
  private[spark] def classTagFor[T: TypeTag] = {
    val mirror = typeTag[T].mirror
    val tpe = typeTag[T].tpe
    val cls = mirror.runtimeClass(tpe)
    ClassTag[T](cls)
  }

  case class UDTSerializer[T >: Null: AvroRecordCodec] private[spark] (
    udt: GeoTrellisUDT[T], child: Expression)
    extends UnaryExpression {

    override def dataType: DataType = udt.sqlType

    override protected def nullSafeEval(input: Any): InternalRow = {
      val value = input.asInstanceOf[T]
      udt.serialize(value)
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      nullSafeCodeGen(ctx, ev, c ⇒ {
        val udtName = ctx.addReferenceObj("udt", udt, udt.getClass.getName)
        val userClass = udt.userClass.getName
          s"${ev.value} = ${udtName}.serialize(($userClass)$c);"
      })
    }

    override protected def otherCopyArgs: Seq[AnyRef] = implicitly[AvroRecordCodec[T]] :: Nil

  }

  case class UDTDeserializer[T >: Null: AvroRecordCodec: TypeTag] private[spark] (
    udt: GeoTrellisUDT[T], child: Expression)
    extends UnaryExpression with CodegenFallback {

    override def dataType: DataType = ScalaReflection.dataTypeFor[T]

    override protected def nullSafeEval(input: Any): T  = {
      val row = input.asInstanceOf[InternalRow]
      udt.deserialize(row)
    }
    override protected def otherCopyArgs: Seq[AnyRef] =
      implicitly[AvroRecordCodec[T]] :: typeTag[T] :: Nil
  }
}
