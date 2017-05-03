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
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.summary.Statistics
import geotrellis.raster.mapalgebra.{local ⇒ alg}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Inline}
import org.apache.spark.sql.types.{StructType, UDTRegistration, UserDefinedType}
import org.apache.spark.sql._

import scala.reflect.runtime.universe._
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

  /** Create a row for each pixel in tile. */
  def explodeTile(cols: Column*): Column = explodeAndSampleTile(1.0, cols: _*)

  /** Create a row for each pixel in tile with random sampling. */
  def explodeAndSampleTile(sampleFraction: Double, cols: Column*): Column = {
    val exploder = ExplodeTileExpression(sampleFraction, cols.map(_.expr))
    // Hack to grab the first two non-cell columns
    val metaNames = exploder.elementSchema.fieldNames.take(2)
    val colNames = cols.map(_.expr).map {
      case ua: UnresolvedAttribute ⇒ ua.name
      case o ⇒ o.prettyName
    }

    Column(exploder).as(metaNames ++ colNames)
  }

  /** Query the number of rows in a tile. */
  def gridRows(col: Column) = withAlias("gridRows", col)(
    SparkUDF[Int, Tile](UDFs.gridRows).apply(col)
  ).as[Int]

  /** Query the number of columns in a tile. */
  def gridCols(col: Column) = withAlias("gridCols", col)(
    SparkUDF[Int, Tile](UDFs.gridCols).apply(col)
  ).as[Int]

  /** Compute the focal sum of a tile with the given radius. */
  def focalSum(tile: Column, size: Column) = withAlias("focalSum", tile)(
    SparkUDF[Tile, Tile, Int](UDFs.focalSum).apply(tile, size)
  )

  /** Compute the cellwise/local max operation between tiles in a column. */
  def localMax(col: Column) = withAlias("localMax", col)(
    UDFs.localMax(col)
  ).as[Tile]

  /** Compute the cellwise/local min operation between tiles in a column. */
  def localMin(col: Column) = withAlias("localMin", col)(
    UDFs.localMin(col)
  ).as[Tile]

  /** Cellwise addition between two tiles. */
  def localAdd(left: Column, right: Column) = localAlgebra(alg.Add, left, right)

  /** Cellwise subtraction between two tiles. */
  def localSubtract(left: Column, right: Column) = localAlgebra(alg.Subtract, left, right)

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two tile columns. */
  def localAlgebra(op: LocalTileBinaryOp, left: Column, right: Column) =
    withAlias(opName(op), left, right)(
      SparkUDF[Tile, Tile, Tile](op.apply).apply(left, right)
    ).as[Tile]

  /** Compute tileHistogram of floating point tile values. */
  def tileHistogramDouble(col: Column) = withAlias("tileHistogramDouble", col)(
    SparkUDF[Histogram[Double], Tile](UDFs.tileHistogramDouble).apply(col)
  ).as[Histogram[Double]]

  /** Compute statistics of tile values. */
  def tileStatisticsDouble(col: Column) = withAlias("tileStatisticsDouble", col)(
    SparkUDF[Statistics[Double], Tile](UDFs.tileStatisticsDouble).apply(col)
  ).as[Statistics[Double]]

  /** Compute the tile-wise mean */
  def tileMeanDouble(col: Column) = withAlias("tileMeanDouble", col)(
    SparkUDF[Double, Tile](UDFs.tileMeanDouble).apply(col)
  ).as[Double]

  /** Compute the tile-wise mean */
  def tileMean(col: Column) = withAlias("tileMean", col)(
    SparkUDF[Double, Tile](UDFs.tileMean).apply(col)
  ).as[Double]

  /** Compute tileHistogram of tile values. */
  def tileHistogram(col: Column) = withAlias("tileHistogram", col)(
    SparkUDF[Histogram[Int], Tile](UDFs.tileHistogram).apply(col)
  ).as[Histogram[Int]]

  /** Compute statistics of tile values. */
  def tileStatistics(col: Column) = withAlias("tileStatistics", col)(
    SparkUDF[Statistics[Int], Tile](UDFs.tileStatistics).apply(col)
  ).as[Statistics[Int]]

  def histogram(col: Column) = withAlias("histogramDouble", col)(
    UDFs.histogram(col)
  ).as[Histogram[Double]]

  /** Render tile as ASCII string for debugging purposes. */
  def renderAscii(col: Column) = withAlias("renderAscii", col)(
    SparkUDF[String, Tile](UDFs.renderAscii).apply(col)
  ).as[String]

  // -- Private APIs below --
  private[gt] def colName(c: Column) = c.expr match {
    case ua: UnresolvedAttribute ⇒ ua.name
    case o ⇒ o.prettyName
  }

  /** Tags output column with a nicer name. */
  private[gt] def withAlias(name: String, inputs: Column*)(output: Column) = {
    val paramNames = inputs.map(colName).mkString(",")
    output.as(s"$name($paramNames)")
  }

  private[gt] def opName(op: LocalTileBinaryOp) =
    op.getClass.getSimpleName.replace("$", "").toLowerCase

  /** Lookup the registered Catalyst UDT for the given Scala type. */
  private[gt] def udtOf[T >: Null: TypeTag]: UserDefinedType[T] =
    UDTRegistration.getUDTFor(typeTag[T].tpe.toString).map(_.newInstance().asInstanceOf[UserDefinedType[T]])
      .getOrElse(throw new IllegalArgumentException(typeTag[T].tpe + " doesn't have a corresponding UDT"))

  /** Creates a Catalyst expression for flattening the fields in a struct into columns. */
  private[gt] def projectStructExpression(dataType: StructType, input: Expression) =
    MultiAlias(Inline(CreateArray(Seq(input))), dataType.fields.map(_.name))
}
