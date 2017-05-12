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

import geotrellis.raster
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{ArrayTile, IntArrayTile, IntConstantNoDataArrayTile, IntConstantNoDataCellType, IntConstantTile, Tile, isNoData}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._

/**
 * Aggregation function for computing multiple local (cell-wise) statistics across all tiles.
 *
 * @author sfitch 
 * @since 4/17/17
 */
class StatsLocalTileAggregateFunction() extends UserDefinedAggregateFunction {
  import StatsLocalTileAggregateFunction._
  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def dataType: DataType = StructType(Seq(
    StructField("count", TileUDT),
    StructField("min", TileUDT),
    StructField("max", TileUDT),
    StructField("mean", TileUDT),
    StructField("variance", TileUDT)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("count", TileUDT),
    StructField("min", TileUDT),
    StructField("max", TileUDT),
    StructField("sum", TileUDT),
    StructField("sumSqr", TileUDT)
  ))

  private val stats = Seq(
    (t1: Tile, t2: Tile) ⇒ Add(t1, Defined(t2)),
    (t1: Tile, t2: Tile) ⇒ BiasedMin(t1, t2),
    (t1: Tile, t2: Tile) ⇒ BiasedMax(t1, t2),
    (t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, t2),
    (t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, Multiply(t2, t2))
  )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = ()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val tile = input.getAs[Tile](0)
    if(tile != null) {
      if(buffer(0) == null) {
        buffer(0) = IntArrayTile.fill(1, tile.cols, tile.rows, IntConstantNoDataCellType)
        buffer(1) = tile
        buffer(2) = tile
        buffer(3) = tile
        buffer(4) = tile
      }
      else {
        for(i ← stats.indices) {
          buffer(i) = stats(i)(buffer.getAs[Tile](i), tile)
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val pickOrApply = (t1: Tile, t2: Tile, op: (Tile, Tile) ⇒ Tile) ⇒
      if(t1 == null) t2 else if(t2 == null) t1 else op(t1, t2)

    for(i ← stats.indices) {
      buffer1(i) = pickOrApply(buffer1.getAs[Tile](i), buffer2.getAs[Tile](i), stats(i))
    }
  }

  override def evaluate(buffer: Row): Row = {
    val count = buffer.getAs[Tile](0)
    if(count != null) {
      val sum = buffer.getAs[Tile](3)
      val sumSqr = buffer.getAs[Tile](4)
      val mean = sum / count
      val variance = sumSqr / count - mean * mean
      Row(buffer(0), buffer(1), buffer(2), mean, variance)
    }
    else null
  }
}

object StatsLocalTileAggregateFunction {

  trait BiasedOp extends LocalTileBinaryOp {
    def op(z1: Int, z2: Int): Int
    def op(z1: Double, z2: Double): Double

    def combine(z1:Int,z2:Int): Int =
      if (isNoData(z1) && isNoData(z2)) raster.NODATA
      else if(isNoData(z1)) z2 else if(isNoData(z2)) z1
      else op(z1,z2)

    def combine(z1:Double,z2:Double): Double =
      if (isNoData(z1) && isNoData(z2)) raster.doubleNODATA
      else if(isNoData(z1)) z2 else if(isNoData(z2)) z1
      else op(z1,z2)
  }

  object BiasedMin extends BiasedOp {
    def op(z1: Int, z2: Int) =  math.min(z1,z2)
    def op(z1: Double, z2: Double) =  math.min(z1,z2)
  }

  object BiasedMax extends BiasedOp {
    def op(z1: Int, z2: Int) =  math.max(z1,z2)
    def op(z1: Double, z2: Double) =  math.max(z1,z2)
  }

  object BiasedAdd extends BiasedOp {
    def op(z1: Int, z2: Int) =  z1 + z2
    def op(z1: Double, z2: Double) =  z1 + z2
  }
}
