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

import geotrellis.raster.{BitCellType, BitConstantTile, ByteCells, ByteConstantTile, CellGrid, CellType, DoubleCells, DoubleConstantTile, FloatCells, FloatConstantTile, IntCells, IntConstantTile, ShortCells, ShortConstantTile, Tile, UByteCells, UByteConstantTile, UShortCells, UShortConstantTile}
import geotrellis.raster.mapalgebra.focal.{Square, Sum}
import geotrellis.raster.mapalgebra.local.Max

/**
 * Library of simple GT-related UDFs.
 *
 * @author sfitch 
 * @since 4/12/17
 */
object UDFs {

  /** Render tile as ASCII string. */
  private[gt] val renderAscii: (Tile) ⇒ String = tile ⇒ tile.asciiDraw()

  /** Constructor for constant tiles */
  private[gt] val makeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
    val cellType = CellType.fromString(cellTypeName)
    cellType match {
      case BitCellType => BitConstantTile(if (value.intValue() == 0) false else true, cols, rows)
      case ct: ByteCells => ByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: UByteCells => UByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: ShortCells => ShortConstantTile(value.shortValue() , cols, rows, ct)
      case ct: UShortCells =>  UShortConstantTile(value.shortValue() , cols, rows, ct)
      case ct: IntCells =>  IntConstantTile(value.intValue() , cols, rows, ct)
      case ct: FloatCells => FloatConstantTile(value.floatValue() , cols, rows, ct)
      case ct: DoubleCells => DoubleConstantTile(value.doubleValue(), cols, rows, ct)
    }
  }

  private[gt] val makeTiles: (Int) ⇒ Array[Tile] = (count) ⇒
    Array.fill(count)(makeConstantTile(0, 4, 4, "int8raw"))

  private[gt] val gridCols: (CellGrid) ⇒ (Int) = (tile) ⇒ tile.cols
  private[gt] val gridRows: (CellGrid) ⇒ (Int) = (tile) ⇒ tile.rows

  /** Perform a focal sum over square area with given half/width extent (value of 1 would be a 3x3 tile) */
  private[gt] val focalSum: (Tile, Int) ⇒ Tile = (tile, extent) ⇒ Sum(tile, Square(extent))
}
