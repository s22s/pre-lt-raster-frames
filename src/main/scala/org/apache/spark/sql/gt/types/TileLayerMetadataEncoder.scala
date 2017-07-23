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

import geotrellis.spark.{KeyBounds, TileLayerMetadata}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.expressions.{If, _}
import org.apache.spark.sql.gt.Implicits._
import org.apache.spark.sql.types._

import scala.reflect.classTag
import scala.reflect.runtime.universe.{Literal ⇒ _, _}

/**
 *
 * @author sfitch 
 * @since 7/21/17
 */
object TileLayerMetadataEncoder {

  private def fieldEncoders = Seq[(String, ExpressionEncoder[_])](
    "cellType" -> cellTypeEncoder,
    "layout" -> layoutDefinitionEncoder,
    "extent" -> extentEncoder,
    "crs" -> crsEncoder
  )

  def apply[K: Encoder: TypeTag](): Encoder[TileLayerMetadata[K]] = {

    val boundsEncoder = ExpressionEncoder[KeyBounds[K]]()

    val fEncoders = fieldEncoders :+ ("bounds" -> boundsEncoder)

    val schema = StructType(
      fEncoders.map{ case (name, encoder) ⇒
        StructField(name, encoder.schema, false)
      }
    )

    val tlmType = ScalaReflection.dataTypeFor[TileLayerMetadata[K]]

    val inputObject = BoundReference(0, tlmType, nullable = false)

    val serializer = CreateNamedStruct(
      fEncoders.flatMap { case (name, encoder) ⇒
        val enc = encoder.serializer.map(_.transform {
          case r: BoundReference if r != inputObject ⇒
            Invoke(inputObject, name, r.dataType)
        })
        Literal(name) :: CreateStruct(enc) :: Nil
      }
    )

    val fieldDeserializers = fEncoders.map(_._2)
      .zipWithIndex.map { case (enc, index) =>
      //enc.pprint()
      if (enc.flat) {
        enc.deserializer.transform {
          case g: GetColumnByOrdinal => g.copy(ordinal = index)
        }
      } else {
        val input = GetColumnByOrdinal(index, enc.schema)
        val deserialized = enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) =>
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
        }
        If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
      }
    }

    val deserializer: Expression = NewInstance(classOf[TileLayerMetadata[K]],
      fieldDeserializers,
      tlmType,
      propagateNull = false
    )

    ExpressionEncoder[TileLayerMetadata[K]](
      schema,
      flat = false,
      serializer.flatten,
      deserializer,
      classTag[TileLayerMetadata[K]]
    )
  }
}
