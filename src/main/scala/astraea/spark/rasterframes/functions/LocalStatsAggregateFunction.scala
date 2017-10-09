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

package astraea.spark.rasterframes.functions

import java.lang.Double

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{DoubleConstantNoDataCellType, IntConstantNoDataCellType, Tile, isNoData}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._
import DataBiasedOp._


/**
 * Aggregation function for computing multiple local (cell-wise) statistics across all tiles.
 *
 * @author sfitch
 * @since 4/17/17
 */
class LocalStatsAggregateFunction() extends UserDefinedAggregateFunction {

  private val reafiableUDT = new TileUDT()

  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("count", reafiableUDT),
        StructField("min", reafiableUDT),
        StructField("max", reafiableUDT),
        StructField("mean", reafiableUDT),
        StructField("variance", reafiableUDT)
      )
    )

  override def bufferSchema: StructType =
    StructType(
      Seq(
        StructField("count", TileUDT),
        StructField("min", TileUDT),
        StructField("max", TileUDT),
        StructField("sum", TileUDT),
        StructField("sumSqr", TileUDT)
      )
    )

  private val initFunctions = Seq(
    (t: Tile) ⇒ Defined(t).convert(IntConstantNoDataCellType),
    (t: Tile) ⇒ t,
    (t: Tile) ⇒ t,
    (t: Tile) ⇒ t.convert(DoubleConstantNoDataCellType),
    (t: Tile) ⇒ { val d = t.convert(DoubleConstantNoDataCellType); Multiply(d, d) }
  )

  private val updateFunctions = Seq(
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, Defined(t2))),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedMin(t1, t2)),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedMax(t1, t2)),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, t2)),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, Multiply(t2, t2)))
  )

  private val mergeFunctions = Seq(
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, t2)),
    updateFunctions(1),
    updateFunctions(2),
    updateFunctions(3),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, t2))
  )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for(i ← initFunctions.indices) {
      buffer(i) = null
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val right = input.getAs[Tile](0)
    if (right != null) {
      if (buffer(0) == null) {
        for (i ← initFunctions.indices) {
          buffer(i) = initFunctions(i)(right)
        }
      } else {
        for (i ← updateFunctions.indices) {
          val left = buffer.getAs[Tile](i)
          buffer(i) = updateFunctions(i)(left, right)
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    for (i ← mergeFunctions.indices) {
      val left = buffer1.getAs[Tile](i)
      val right = buffer2.getAs[Tile](i)
      buffer1(i) = mergeFunctions(i)(left, right)
    }
  }

  override def evaluate(buffer: Row): Any = {
    val cnt = buffer.getAs[Tile](0)
    if (cnt != null) {
      val count = cnt.convert(DoubleConstantNoDataCellType)
      val sum = buffer.getAs[Tile](3)
      val sumSqr = buffer.getAs[Tile](4)
      val mean = sum / count
      val variance = (sumSqr / count) - (mean * mean)
      Row(cnt, buffer(1), buffer(2), mean, variance)
    } else null
  }
}
