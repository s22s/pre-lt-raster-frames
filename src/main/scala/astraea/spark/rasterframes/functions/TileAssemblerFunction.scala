/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package astraea.spark.rasterframes.functions

import geotrellis.raster.{ArrayTile, CellType, DoubleConstantNoDataCellType, MutableArrayTile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._

/**
 * Aggregator for reassembling tiles from from exploded form
 *
 * @author sfitch 
 * @since 9/24/17
 */
class TileAssemblerFunction(cols: Int, rows: Int, ct: CellType) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Seq(
    StructField("columnIndex", IntegerType, false),
    StructField("rowIndex", IntegerType, false),
    StructField("cellValues", DoubleType, false)
  ))

  def bufferSchema: StructType = StructType(Seq(
    StructField("cells",
      DataTypes.createMapType(IntegerType,
        DataTypes.createMapType(IntegerType, DoubleType, false), false),
      false)
  ))

  def dataType: DataType = new TileUDT()

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map.empty[Int, Map[Int, Double]]
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val col = input.getInt(0)
    val row = input.getInt(1)
    val cell = input.getDouble(2)

    val columns = buffer.getAs[Map[Int, Map[Int, Double]]](0)
    val rows = columns.getOrElse(col, Map.empty[Int, Double])
    val updatedRows = rows + (row -> cell)
    val updatedColumns = columns + (col -> updatedRows)
    buffer(0) = updatedColumns
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var destColumns = buffer1.getAs[Map[Int, Map[Int, Double]]](0)
    val sourceColumns = buffer2.getAs[Map[Int, Map[Int, Double]]](0)

    for {
      (col, map) ← sourceColumns
      (row, cell) ← map
    } {
      val rows = destColumns.getOrElse(col, Map.empty[Int, Double])
      val updatedRows = rows + (row -> cell)
      destColumns = destColumns + (col -> updatedRows)
    }
    buffer1(0) = destColumns
  }

  def evaluate(buffer: Row): Any = {
    val columns = buffer.getAs[Map[Int, Map[Int, Double]]](0)

    // We assume that the shape of the tile is associated with the found
    // keys. Which is probably not a good assumption since ND could have been
    // filtered out.
    val maxCol = columns.keys.max
    val maxRow = columns.values.map(_.keys.max).max

    val retval = ArrayTile.empty(DoubleConstantNoDataCellType, maxCol + 1, maxRow + 1)

    for {
      (col, map) ← columns
      (row, cell) ← map
    } retval.setDouble(col, row, cell)

    retval
  }
}
