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

import geotrellis.raster.histogram.{DoubleHistogram, Histogram, StreamingHistogram}
import geotrellis.raster.{ArrayTile, BitCellType, BitConstantTile, ByteCellType, ByteCells, ByteConstantNoDataCellType, ByteConstantTile, CellGrid, CellType, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleConstantTile, FloatCellType, FloatCells, FloatConstantNoDataCellType, FloatConstantTile, IntCellType, IntCells, IntConstantNoDataCellType, IntConstantTile, ShortCellType, ShortCells, ShortConstantNoDataCellType, ShortConstantTile, Tile, UByteCellType, UByteCells, UByteConstantNoDataCellType, UByteConstantTile, UShortCellType, UShortCells, UShortConstantNoDataCellType, UShortConstantTile}
import geotrellis.raster.mapalgebra.focal.{Square, Sum}
import geotrellis.raster.mapalgebra.local.{Max, Min}

import scala.util.Random

/**
 * Library of simple GT-related UDFs.
 *
 * @author sfitch 
 * @since 4/12/17
 */
object UDFs {

  /** Reports number of columns in a tile. */
  private[gt] val gridCols: (CellGrid) ⇒ (Int) = (tile) ⇒ tile.cols
  /** Reports number of rows in a tile. */
  private[gt] val gridRows: (CellGrid) ⇒ (Int) = (tile) ⇒ tile.rows

  private[gt] val histogram: (Tile) ⇒ Histogram[Double] = (tile) ⇒ StreamingHistogram.fromTile(tile)

  /** Perform a focal sum over square area with given half/width extent (value of 1 would be a 3x3 tile) */
  private[gt] val focalSum: (Tile, Int) ⇒ Tile = (tile, extent) ⇒ Sum(tile, Square(extent))
  /** Compute the cell-wise max across tiles. */
  private[gt] val localMax = new LocalTileAggregateFunction(Max)
  /** Compute the cell-wise min across tiles. */
  private[gt] val localMin = new LocalTileAggregateFunction(Min)

  /** Render tile as ASCII string. */
  private[gt] val renderAscii: (Tile) ⇒ String = tile ⇒ tile.asciiDraw()

  private[gt] val cellTypes: () ⇒ Seq[String] = () ⇒ Seq(
    BitCellType, ByteCellType, ByteConstantNoDataCellType, UByteCellType, UByteConstantNoDataCellType,
    ShortCellType, ShortConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType,
    IntCellType, IntConstantNoDataCellType,
    FloatCellType, FloatConstantNoDataCellType, DoubleCellType, DoubleConstantNoDataCellType
  ).map(_.toString).distinct

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

  /** Construct a tile of given size and cell type populated with random values. */
  private[gt] val randomTile: (Int, Int, String) ⇒ Tile = (cols, rows, cellTypeName) ⇒ {
    val cellType = CellType.fromString(cellTypeName)

    val tile = ArrayTile.alloc(cellType, cols, rows)
    if(cellType.isFloatingPoint) {
      tile.mapDouble(_ ⇒ Random.nextGaussian())
    }
    else {
      tile.map(_ ⇒ (Random.nextGaussian() * 256).toInt)
    }
  }

  /** Create a series of random tiles. */
  private[gt] val makeTiles: (Int) ⇒ Array[Tile] = (count) ⇒
    Array.fill(count)(randomTile(4, 4, "int8raw"))



}
