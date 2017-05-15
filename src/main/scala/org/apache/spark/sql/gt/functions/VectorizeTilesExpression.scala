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

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Catalyst expression for converting multiple tile columns into a vector column, where each element of the vector
 * is a cell from each of the tiles. This is useful for feature vector creation in SparkML
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[spark] case class VectorizeTilesExpression(sampleFraction: Double = 1.0,
  override val children: Seq[Expression])
  extends Expression with Generator with CodegenFallback {

  override def elementSchema: StructType = {
    StructType(Seq(
      StructField("column", IntegerType, false),
      StructField("row", IntegerType, false),
      StructField("cells", VectorType, false)
    ))
  }

  private def keep(): Boolean = {
    if(sampleFraction >= 1.0) true
    else Utils.random.nextDouble() <= sampleFraction
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val tiles = for(child ← children) yield
      TileUDT.deserialize(child.eval(input).asInstanceOf[InternalRow])

    require(tiles.map(_.dimensions).distinct.size == 1, "Multi-column vectorization requires equally sized tiles")

    val (cols, rows) = tiles.head.dimensions

    for {
      row ← 0 until rows
      col ← 0 until cols
      if keep()
      vector = org.apache.spark.ml.linalg.Vectors.dense(tiles.map(_.getDouble(col, row)).toArray)
    } yield InternalRow(row, col, VectorType.asInstanceOf[VectorUDT].serialize(vector))
  }
}
