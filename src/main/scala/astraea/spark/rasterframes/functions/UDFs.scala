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

import astraea.spark.rasterframes.HasCellType
import geotrellis.raster._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.summary.Statistics

import scala.reflect.runtime.universe._

/**
 * Library of simple GT-related UDFs.
 *
 * @author sfitch
 * @since 4/12/17
 */
object UDFs {

  private[rasterframes] def safeEval[P, R](f: P ⇒ R): P ⇒ R =
    (p) ⇒ if (p == null) null.asInstanceOf[R] else f(p)
  private[rasterframes] def safeEval[P1, P2, R](f: (P1, P2) ⇒ R): (P1, P2) ⇒ R =
    (p1, p2) ⇒ if (p1 == null || p2 == null) null.asInstanceOf[R] else f(p1, p2)

  /** Flattens tile into an array. */
  private[rasterframes] def tileToArray[T: HasCellType: TypeTag] =
    safeEval[Tile, Array[T]] { tile ⇒
      val asArray = tile match {
        case t: IntArrayTile ⇒ t.array
        case t: DoubleArrayTile ⇒ t.array
        case t: ByteArrayTile ⇒ t.array
        case t: ShortArrayTile ⇒ t.array
        case t: FloatArrayTile ⇒ t.array
        case o: Tile ⇒ typeOf[T] match {
          case t if t =:= typeOf[Int] ⇒ o.toArray()
          case t if t =:= typeOf[Double] ⇒ o.toArrayDouble()
          case t if t =:= typeOf[Byte] ⇒ o.toArray().map(_.toByte)
          case t if t =:= typeOf[Short] ⇒ o.toArray().map(_.toShort)
          case t if t =:= typeOf[Float] ⇒ o.toArrayDouble().map(_.toFloat)
        }
      }
      asArray.asInstanceOf[Array[T]]
    }

  /** Converts an array into a tile. */
  private[rasterframes] def arrayToTile(cols: Int, rows: Int) = {
    safeEval[AnyRef, Tile]{
      case s: Seq[_] ⇒ s.headOption match {
        case Some(_: Int) ⇒ RawArrayTile(s.asInstanceOf[Seq[Int]].toArray[Int], cols, rows)
        case Some(_: Double) ⇒ RawArrayTile(s.asInstanceOf[Seq[Double]].toArray[Double], cols, rows)
        case Some(_: Byte) ⇒ RawArrayTile(s.asInstanceOf[Seq[Byte]].toArray[Byte], cols, rows)
        case Some(_: Short) ⇒ RawArrayTile(s.asInstanceOf[Seq[Short]].toArray[Short], cols, rows)
        case Some(_: Float) ⇒ RawArrayTile(s.asInstanceOf[Seq[Float]].toArray[Float], cols, rows)
        case Some(o @ _) ⇒ throw new MatchError(o)
        case None ⇒ null
      }
    }
  }

  private[rasterframes] def assembleTile(cols: Int, rows: Int, ct: CellType) = new TileAssemblerFunction(cols, rows, ct)

  /** Computes the column aggregate histogram */
  private[rasterframes] val aggHistogram = new AggregateHistogramFunction()

  /** Computes the column aggregate statistics */
  private[rasterframes] val aggStats = new AggregateStatsFunction()

  /** Reports the dimensions of a tile. */
  private[rasterframes] val tileDimensions = safeEval[CellGrid, (Int, Int)](_.dimensions)

  /** Get the tile's cell type*/
  private[rasterframes] val cellType = safeEval[Tile, String](_.cellType.name)

  /** Set the tile's no-data value. */
  private[rasterframes] def withNoData(nodata: Double) = safeEval[Tile, Tile](_.withNoData(Some(nodata)))

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
  private[rasterframes] val localAdd: (Tile, Tile) ⇒ Tile = safeEval(Add.apply)

  /** Cell-wise subtraction between tiles. */
  private[rasterframes] val localSubtract: (Tile, Tile) ⇒ Tile = safeEval(Subtract.apply)

  /** Cell-wise multiplication between tiles. */
  private[rasterframes] val localMultiply: (Tile, Tile) ⇒ Tile = safeEval(Multiply.apply)

  /** Cell-wise division between tiles. */
  private[rasterframes] val localDivide: (Tile, Tile) ⇒ Tile = safeEval(Divide.apply)

  /** Render tile as ASCII string. */
  private[rasterframes] val renderAscii: (Tile) ⇒ String = safeEval(_.asciiDraw)

  /** Count tile cells that have a data value. */
  // TODO: Do we need to `safeEval` here?
  // TODO: Should we get rid of the `var`?
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
