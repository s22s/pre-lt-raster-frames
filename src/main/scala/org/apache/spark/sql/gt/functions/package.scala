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

package org.apache.spark.sql.gt

import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Inline}
import org.apache.spark.sql.types.{StructType, UDTRegistration, UserDefinedType}
import org.apache.spark.sql.{Column, Row, TypedColumn}

import scala.reflect.runtime.universe._
import scala.util.Try

/**
 * GT functions adapted for Spark SQL use.
 *
 * @author sfitch
 * @since 4/3/17
 */
package object functions {
  /** Create columns for each field in the structure or UDT. */
  def flatten[T >: Null: TypeTag](col: TypedColumn[_, T]) = Column(
    Try(asStruct[T](col))
      .map(col ⇒ projectStructExpression(col.encoder.schema, col.expr))
      .getOrElse(projectStructExpression(col.encoder.schema, col.expr))
  )

  /** Attempts to convert a UDT into a struct based on the underlying deserializer. */
  def asStruct[T >: Null: TypeTag](col: TypedColumn[_, T]) = {
    val converter = UDTAsStructExpression(udtOf[T], col.expr)
    Column(converter).as[Row](RowEncoder(converter.dataType))
  }

  /** Create a row for each pixel in tile. */
  def explodeTile(cols: Column*) = {
    val exploder = ExplodeTileExpression(cols.map(_.expr))
    // Hack to grab the first two non-cell columns
    val metaNames = exploder.elementSchema.fieldNames.take(2)
    val colNames = cols.map(_.expr).map {
      case ua: UnresolvedAttribute ⇒ ua.name
      case o ⇒ o.prettyName
    }

    Column(exploder).as(metaNames ++ colNames)
  }

  // -- Private APIs below --
  /** Lookup the registered Catalyst UDT for the given Scala type. */
  private[gt] def udtOf[T >: Null: TypeTag]: UserDefinedType[T] =
    UDTRegistration.getUDTFor(typeTag[T].tpe.toString).map(_.newInstance().asInstanceOf[UserDefinedType[T]])
      .getOrElse(throw new IllegalArgumentException(typeTag[T].tpe + " doesn't have a corresponding UDT"))

  /** Creates a Catalyst expression for flattening the fields in a UDT into columns. */
  private[gt] def flattenExpression[T >: Null : TypeTag](input: Expression) = {
    val converter = UDTAsStructExpression(udtOf[T], input)
    projectStructExpression(converter.dataType, converter)
  }

  /** Creates a Catalyst expression for flattening the fields in a struct into columns. */
  private[gt] def projectStructExpression(dataType: StructType, input: Expression) =
    MultiAlias(Inline(CreateArray(Seq(input))), dataType.fields.map(_.name))
}
