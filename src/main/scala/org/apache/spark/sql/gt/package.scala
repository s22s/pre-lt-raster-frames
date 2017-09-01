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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}

/**
 * Module providing support for using GeoTrellis native types in Spark SQL.
 *
 * @author sfitch
 * @since 3/30/17
 */
package object gt {
  @Experimental
  def gtRegister(sqlContext: SQLContext): Unit = {
    gt.types.Registrator.register()
    gt.functions.Registrator.register(sqlContext)
  }

  implicit class NamedColumn(col: Column) {
    def columnName: String = col.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }

  private[gt] def safeBinaryOp[T <: AnyRef, R >: T](op: (T, T) ⇒ R): ((T, T) ⇒ R) =
    (o1: T, o2: T) ⇒ {
      if (o1 == null) o2
      else if (o2 == null) o1
      else op(o1, o2)
    }

  private[gt] implicit class WithDecoder[T](enc: ExpressionEncoder[T]) {
    def decode(row: InternalRow): T =
      enc.resolveAndBind(enc.schema.toAttributes).fromRow(row)
    def decode(row: InternalRow, ordinal: Int): T =
      decode(row.getStruct(ordinal, enc.schema.length))

    def pprint(): Unit = {
      println(enc.getClass.getSimpleName + "{")
      println("\tflat=" + enc.flat)
      println("\tschema=" + enc.schema)
      println("\tserializers=" + enc.serializer)
      println("\tnamedExpressions=" + enc.namedExpressions)
      println("\tdeserializer=" + enc.deserializer)
      println("}")
    }
  }
}
