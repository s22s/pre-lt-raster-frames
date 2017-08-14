package org.apache.spark.sql.gt.functions

import geotrellis.raster.mapalgebra.local.{Add, Defined}
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.safeBinaryOp
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Catalyst aggregate function that counts non-`NoData` values in a cell-wise fashion.
 *
 * @author sfitch 
 * @since 8/11/17
 */
class LocalCountAggregateFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  private val incCount = safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, Defined(t2)))
  private val add = safeBinaryOp(Add.apply(_: Tile, _: Tile))


  override def dataType: DataType = new TileUDT()

  override def bufferSchema: StructType = inputSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val right = input.getAs[Tile](0)
    if(right != null) {
      if(buffer(0) == null) {
        buffer(0) = Defined(right).convert(IntConstantNoDataCellType)
      }
      else {
        val left = buffer.getAs[Tile](0)
        buffer(0) = incCount(left, right)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = add(buffer1.getAs[Tile](0), buffer2.getAs[Tile](0))
  }

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}
