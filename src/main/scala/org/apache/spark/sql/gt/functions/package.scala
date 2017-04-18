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

import geotrellis.raster.Tile
import geotrellis.raster.histogram.Histogram
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Inline}
import org.apache.spark.sql.types.{StructType, UDTRegistration, UserDefinedType}
import org.apache.spark.sql._

import scala.reflect.runtime.universe._
import scala.util.Try
import org.apache.spark.sql.functions.{udf ⇒ SparkUDF}

/**
 * GT functions adapted for Spark SQL use.
 *
 * @author sfitch
 * @since 4/3/17
 */
package object functions {
  private val encoders = new SQLImplicits {
    override protected def _sqlContext: SQLContext = ???
  }
  import encoders._

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

  /** Compute the cellwise/local max operation between tiles in a column. */
  def localMax(col: Column) = withAlias("localMax", col, UDFs.localMax(col)).as[Tile]
  /** Compute the cellwise/local min operation between tiles in a column. */
  def localMin(col: Column) = withAlias("localMin", col, UDFs.localMin(col)).as[Tile]
  /** Compute histogram of tile values. */
  def histogram(col: Column) = withAlias("histogram", col,
    SparkUDF[Histogram[Double], Tile](UDFs.histogram).apply(col)
  ).as[Histogram[Double]]

  /** Render tile as ASCII string for debugging purposes. */
  def renderAscii(col: Column) = withAlias("renderAscii", col,
    SparkUDF[String, Tile](UDFs.renderAscii).apply(col)
  ).as[String]

  // -- Private APIs below --
  /** Tags output column with something resonable. */
  private[gt] def withAlias(name: String, input: Column, output: Column) = {
    val paramName = input.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case o ⇒ o.prettyName
    }
    output.as(s"$name($paramName)")
  }

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
