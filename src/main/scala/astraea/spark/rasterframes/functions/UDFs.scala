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
    (p) ⇒ if (p == null) null.asInstanceOf[R] else f(p)
  private def safeEval[P1, P2, R](f: (P1, P2) ⇒ R): (P1, P2) ⇒ R =
    (p1, p2) ⇒ if (p1 == null || p2 == null) null.asInstanceOf[R] else f(p1, p2)

  /** Flattens tile into an integer array. */
  private[rasterframes] val tileToArray = safeEval[Tile, Array[Int]](_.toArray())

  /** Flattens tile into a double array. */
  private[rasterframes] val tileToArrayDouble = safeEval[Tile, Array[Double]](_.toArrayDouble())

  /** Computes the column aggregate histogram */
  private[rasterframes] val aggHistogram = new AggregateHistogramFunction()

  /** Computes the column aggregate statistics */
  private[rasterframes] val aggStats = new AggregateStatsFunction()

  /** Reports the dimensions of a tile. */
  private[rasterframes] val tileDimensions = safeEval[CellGrid, (Int, Int)](_.dimensions)

  /** Get the tile's cell type*/
  private[rasterframes] val cellType = safeEval[Tile, String](_.cellType.name)

  /** Single floating point tile histogram. */
  private[rasterframes] val tileHistogramDouble = safeEval[Tile, Histogram[Double]](_.histogramDouble())

  /** Single floating point tile statistics. Convenience for `tileHistogram.statisticsDouble`. */
  private[rasterframes] val tileStatsDouble = safeEval[Tile, Statistics[Double]](_.statisticsDouble.orNull)

  /** Single floating point tile mean. Convenience for `tileHistogram.statisticsDouble.mean`. */
  private[rasterframes] val tileMeanDouble = safeEval[Tile, Double](_.statisticsDouble.map(_.mean).getOrElse(Double.NaN))

  /** Single tile histogram. */
  private[rasterframes] val tileHistogram = safeEval[Tile, Histogram[Int]](_.histogram)

  /** Single tile statistics. Convenience for `tileHistogram.statistics`. */
  private[rasterframes] val tileStats = safeEval[Tile, Statistics[Int]](_.statistics.orNull)

  /** Single tile mean. Convenience for `tileHistogram.statistics.mean`. */
  private[rasterframes] val tileMean = safeEval[Tile, Double](_.statistics.map(_.mean).getOrElse(Double.NaN))

  /** Compute summary cell-wise statistics across tiles. */
  private[rasterframes] val localAggStats = new StatsLocalTileAggregateFunction()

  /** Compute the cell-wise max across tiles. */
  private[rasterframes] val localAggMax = new LocalTileOpAggregateFunction(Max)

  /** Compute the cell-wise min across tiles. */
  private[rasterframes] val localAggMin = new LocalTileOpAggregateFunction(Min)

  /** Compute the cell-wise main across tiles. */
  private[rasterframes] val localAggMean = new LocalMeanAggregateFunction()

  /** Compute the cell-wise count of non-NA across tiles. */
  private[rasterframes] val localAggCount = new LocalCountAggregateFunction()

  /** Cell-wise addition between tiles. */
  private[rasterframes] val localAdd: (Tile, Tile) ⇒ Tile = safeEval((left, right) ⇒ Add(left, right))

  /** Cell-wise subtraction between tiles. */
  private[rasterframes] val localSubtract: (Tile, Tile) ⇒ Tile = safeEval((left, right) ⇒ Subtract(left, right))

  /** Render tile as ASCII string. */
  private[rasterframes] val renderAscii: (Tile) ⇒ String = safeEval(_.asciiDraw)

  /** Count tile cells that have a data value. */
  private[rasterframes] val dataCells: (Tile) ⇒ Long = (t: Tile) ⇒ {
    var count: Long = 0
    t.foreach(z ⇒ if(isData(z)) count = count + 1)
    count
  }

  /** Count tile cells that have a no-data value. */
  private[rasterframes] val nodataCells: (Tile) ⇒ Long = (t: Tile) ⇒ {
    var count: Long = 0
    t.foreach(z ⇒ if(isNoData(z)) count = count + 1)
    count
  }

  /** Constructor for constant tiles */
  private[rasterframes] val makeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
    val cellType = CellType.fromName(cellTypeName)
    cellType match {
      case BitCellType ⇒ BitConstantTile(if (value.intValue() == 0) false else true, cols, rows)
      case ct: ByteCells ⇒ ByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: UByteCells ⇒ UByteConstantTile(value.byteValue(), cols, rows, ct)
      case ct: ShortCells ⇒ ShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: UShortCells ⇒ UShortConstantTile(value.shortValue(), cols, rows, ct)
      case ct: IntCells ⇒ IntConstantTile(value.intValue(), cols, rows, ct)
      case ct: FloatCells ⇒ FloatConstantTile(value.floatValue(), cols, rows, ct)
      case ct: DoubleCells ⇒ DoubleConstantTile(value.doubleValue(), cols, rows, ct)
    }
  }
}
