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

package org.apache.spark.sql.gt

import java.nio.ByteBuffer

import geotrellis.raster._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Wrapper around a `Tile` encoded in a Catalyst `InternalRow`, for the purpose
 * of providing compatible semantics over common operations.
 *
 * @groupname COPIES Memory Copying
 * @groupdesc COPIES Requires creating an intermediate copy of
 *           the complete `Tile` contents, and should be avoided.
 *
 * @author sfitch 
 * @since 11/29/17
 */
class InternalRowTile(mem: InternalRow) extends ArrayTile {
  import InternalRowTile.C._

  /** Retrieve the cell type from the internal encoding. */
  val cellType: CellType = CellType.fromName(mem.getString(CELL_TYPE))

  /** Retrieve the number of columns from the internal encoding. */
  val cols: Int = mem.getShort(COLS)

  /** Retrieve the number of rows from the internal encoding. */
  val rows: Int = mem.getShort(ROWS)

  /** Get the internally encoded tile data cells. */
  lazy val toBytes: Array[Byte] = mem.getBinary(DATA)

  private lazy val toByteBuffer: ByteBuffer = {
    val data = toBytes
    if(data.length < cols * rows && cellType.name != "bool") {
      // Handling constant tiles like this is inefficient and ugly. All the edge
      // cases associated with them create too much undue complexity for
      // something that's unlikely to be
      // used much in production to warrant handling them specially.
      // If a more efficient handling is necessary, consider a flag in
      // the UDT struct.
      ByteBuffer.wrap(toArrayTile.toBytes())
    } else ByteBuffer.wrap(data)
  }

  /** Reads the cell value at the given index as an Int. */
  def apply(i: Int): Int = {
    val bb = toByteBuffer
    cellType match {
      case _: BitCells ⇒ (bb.get(i >> 3) >> (i & 7)) & 1 // See BitArrayTile.apply
      case _: ByteCells ⇒ bb.get(i)
      case _: UByteCells ⇒ bb.get(i) & 0xFF
      case _: ShortCells ⇒ bb.asShortBuffer().get(i)
      case _: UShortCells ⇒ bb.asShortBuffer().get(i) & 0xFFFF
      case _: IntCells ⇒ bb.asIntBuffer().get(i)
      case _: FloatCells ⇒ bb.asFloatBuffer().get(i).toInt
      case _: DoubleCells ⇒ bb.asDoubleBuffer().get(i).toInt
    }
  }

  /** Reads the cell value at the given index as a Double. */
  def applyDouble(i: Int): Double = {
    cellType match {
      case _: FloatCells ⇒ toByteBuffer.asFloatBuffer().get(i)
      case _: DoubleCells ⇒ toByteBuffer.asDoubleBuffer().get(i)
      case _ ⇒ apply(i).toDouble
    }
  }

  /** @group COPIES */
  override def toArrayTile: ArrayTile = {
    val data = toBytes
    if(data.length < cols * rows && cellType.name != "bool") {
      val ctile = InternalRowTile.constantTileFromBytes(data, cellType, cols, rows)
      val atile = ctile.toArrayTile()
      atile
    }
    else
      ArrayTile.fromBytes(data, cellType, cols, rows)
  }

  /** @group COPIES */
  def mutable: MutableArrayTile = toArrayTile().mutable

  /** @group COPIES */
  def interpretAs(newCellType: CellType): Tile =
    toArrayTile().interpretAs(newCellType)

  /** @group COPIES */
  def withNoData(noDataValue: Option[Double]): Tile =
    toArrayTile().withNoData(noDataValue)

  /** @group COPIES */
  def copy = new InternalRowTile(mem.copy)
}

object InternalRowTile {
  object C {
    val CELL_TYPE = 0
    val COLS = 1
    val ROWS = 2
    val DATA = 3
  }

  val schema = StructType(Seq(
    StructField("cellType", StringType, false),
    StructField("cols", ShortType, false),
    StructField("rows", ShortType, false),
    StructField("data", BinaryType, false)
  ))

  /**
   * Constructor.
   * @param row
   * @return
   */
  def apply(row: InternalRow): InternalRowTile = new InternalRowTile(row)

  /**
   * Extractor.
   * @param tile
   * @return
   */
  def unapply(tile: Tile): Option[InternalRow] = Some(
    InternalRow(
      UTF8String.fromString(tile.cellType.name),
      tile.cols.toShort,
      tile.rows.toShort,
      tile.toBytes)
  )

  // Temporary, until GeoTrellis 1.2
  // See https://github.com/locationtech/geotrellis/pull/2401
  private[gt] def constantTileFromBytes(bytes: Array[Byte], t: CellType, cols: Int, rows: Int): ConstantTile =
    t match {
      case _: BitCells =>
        BitConstantTile(BitArrayTile.fromBytes(bytes, 1, 1).array(0), cols, rows)
      case ct: ByteCells =>
        ByteConstantTile(ByteArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: UByteCells =>
        UByteConstantTile(UByteArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: ShortCells =>
        ShortConstantTile(ShortArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: UShortCells =>
        UShortConstantTile(UShortArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: IntCells =>
        IntConstantTile(IntArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: FloatCells =>
        FloatConstantTile(FloatArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: DoubleCells =>
        DoubleConstantTile(DoubleArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
    }
}
