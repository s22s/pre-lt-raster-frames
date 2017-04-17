package org.apache.spark.sql.gt.functions

import geotrellis.raster.{BitConstantTile, Tile}
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._

/**
 * Aggregation function for applying a [[LocalTileBinaryOp]] pairwise across all tiles. Assumes Monoid algebra.
 *
 * @author sfitch 
 * @since 4/17/17
 */
class LocalTileAggregateFunction(op: LocalTileBinaryOp) extends UserDefinedAggregateFunction {
  private val EMPTY = BitConstantTile(0, 0, 0)

  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = new TileUDT()

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = EMPTY

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer(0) == EMPTY) {
      buffer(0) = input(0)
    }
    else {
      val t1 = buffer.getAs[Tile](0)
      val t2 = input.getAs[Tile](0)
      buffer(0) = op(t1, t2)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}
