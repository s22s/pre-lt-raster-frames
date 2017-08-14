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

import geotrellis.raster._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.local.{Add, Max, Min, Subtract}
import geotrellis.raster.summary.Statistics

import scala.util.Random

/**
 * Library of simple GT-related UDFs.
 *
 * @author sfitch 
 * @since 4/12/17
 */
object UDFs {

  private def safeEval[P, R](f: P ⇒ R): P ⇒ R =
    (p) ⇒ if(p == null) null.asInstanceOf[R] else f(p)
  private def safeEval[P1, P2, R](f: (P1, P2) ⇒ R): (P1, P2) ⇒ R =
    (p1, p2) ⇒ if(p1 == null || p2 == null) null.asInstanceOf[R] else f(p1, p2)

  /** Reports the dimensions of a tile. */
  private[gt] val tileDimensions: (CellGrid) ⇒ (Int, Int) = safeEval(_.dimensions)

  /** Computes the column aggregate histogram */
  private[gt] val aggHistogram = new AggregateHistogramFunction()
  /** Computes the column aggregate statistics */
  private[gt] val aggStats = new AggregateStatsFunction()

  /** Single floating point tile histogram. */
  private[gt] val tileHistogramDouble = safeEval[Tile, Histogram[Double]](_.histogramDouble())
  /** Single floating point tile statistics. Convenience for `tileHistogram.statisticsDouble`. */
  private[gt] val tileStatsDouble = safeEval[Tile, Statistics[Double]](_.statisticsDouble.orNull)
  /** Single floating point tile mean. Convenience for `tileHistogram.statisticsDouble.mean`. */
  private[gt] val tileMeanDouble = safeEval[Tile, Double](_.statisticsDouble.map(_.mean).getOrElse(Double.NaN))

  /** Single tile histogram. */
  private[gt] val tileHistogram = safeEval[Tile, Histogram[Int]](_.histogram)
  /** Single tile statistics. Convenience for `tileHistogram.statistics`. */
  private[gt] val tileStats = safeEval[Tile, Statistics[Int]](_.statistics.orNull)
  /** Single tile mean. Convenience for `tileHistogram.statistics.mean`. */
  private[gt] val tileMean = safeEval[Tile, Double](_.statistics.map(_.mean).getOrElse(Double.NaN))

  /** Compute summary cell-wise statistics across tiles. */
  private[gt] val localAggStats = new StatsLocalTileAggregateFunction()
  /** Compute the cell-wise max across tiles. */
  private[gt] val localAggMax = new LocalTileOpAggregateFunction(Max)
  /** Compute the cell-wise min across tiles. */
  private[gt] val localAggMin = new LocalTileOpAggregateFunction(Min)
  /** Compute the cell-wise main across tiles. */
  private[gt] val localAggMean = new LocalMeanAggregateFunction()
  /** Compute the cell-wise count of non-NA across tiles. */
  private[gt] val localAggCount = new LocalCountAggregateFunction()

  /** Cell-wise addition between tiles. */
  private[gt] val localAdd: (Tile, Tile) ⇒ Tile = safeEval((left, right) ⇒
    Add(left, right))
  /** Cell-wise subtraction between tiles. */
  private[gt] val localSubtract: (Tile, Tile) ⇒ Tile = safeEval((left, right) ⇒
    Subtract(left, right))

  /** Render tile as ASCII string. */
  private[gt] val renderAscii: (Tile) ⇒ String = safeEval(_.asciiDraw)

  private[gt] val cellTypes: () ⇒ Seq[String] = () ⇒ Seq(
    BitCellType, ByteCellType, ByteConstantNoDataCellType, UByteCellType, UByteConstantNoDataCellType,
    ShortCellType, ShortConstantNoDataCellType, UShortCellType, UShortConstantNoDataCellType,
    IntCellType, IntConstantNoDataCellType,
    FloatCellType, FloatConstantNoDataCellType, DoubleCellType, DoubleConstantNoDataCellType
  ).map(_.toString).distinct

  /** Constructor for constant tiles */
  private[gt] val makeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
    val cellType = CellType.fromName(cellTypeName)
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
    val cellType = CellType.fromName(cellTypeName)

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
