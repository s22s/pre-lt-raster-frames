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

package org.apache.spark.sql.gt.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Catalyst expression for converting a tile column into a pixel column, with each tile pixel occupying a separate row.
 *
 * @author sfitch
 * @since 4/12/17
 */
private[spark] case class ExplodeTileExpression(sampleFraction: Double = 1.0, override val children: Seq[Expression])
    extends Expression
    with Generator
    with CodegenFallback {

  override def elementSchema: StructType = {
    val names =
      if (children.size == 1) Seq("cell")
      else children.indices.map(i ⇒ s"cell_$i")

    StructType(
      Seq(StructField("column", IntegerType, false), StructField("row", IntegerType, false)) ++ names
        .map(n ⇒ StructField(n, DoubleType, false))
    )
  }

  private def keep(): Boolean = {
    if (sampleFraction >= 1.0) true
    else Utils.random.nextDouble() <= sampleFraction
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    // Do we need to worry about deserializing all the tiles in a row like this?
    val tiles = for (child ← children) yield TileUDT.deserialize(child.eval(input).asInstanceOf[InternalRow])

    require(tiles.map(_.dimensions).distinct.size == 1, "Multi-column explode requires equally sized tiles")

    val (cols, rows) = tiles.head.dimensions

    for {
      row ← 0 until rows
      col ← 0 until cols
      if keep()
      contents = Seq[Any](col, row) ++ tiles.map(_.getDouble(col, row))
    } yield InternalRow(contents: _*)
  }
}
