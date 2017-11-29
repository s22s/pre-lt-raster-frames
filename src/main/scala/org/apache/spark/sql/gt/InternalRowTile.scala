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
import org.apache.spark.sql.gt.types.TileUDT

import scala.reflect.macros.whitebox

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
  import org.apache.spark.sql.gt.types.TileUDT.C._

  /** Retrieve the cell type from the internal encoding. */
  def cellType: CellType = CellType.fromName(mem.getString(CELL_TYPE))

  /** Retrieve the number of columns from the internal encoding. */
  def cols: Int = mem.getShort(COLS)

  /** Retrieve the number of rows from the internal encoding. */
  def rows: Int = mem.getShort(ROWS)

  /** Get the internally encoded tile data cells. */
  def toBytes(): Array[Byte] = mem.getBinary(DATA)

  private def toByteBuffer: ByteBuffer = {
    val data = toBytes()
    ByteBuffer.wrap(data)
  }

  /** Reads the cell value at the given index as an Int. */
  def apply(i: Int): Int = {
    val bb = toByteBuffer
    cellType match {
      // This case isn't worth the effort to decode directly
      case _: BitCells ⇒  BitArrayTile.fromBytes(bb.array(), cols, rows)(i)
      case _: ByteCells ⇒ bb.get(i)
      case _: UByteCells ⇒ bb.asIntBuffer().get(i)
      case _: ShortCells ⇒ bb.asShortBuffer().get(i)
      case _: UShortCells ⇒ bb.asIntBuffer().get(i)
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
    val data = toBytes()
    if(data.length < cols * rows && !cellType.isInstanceOf[BitCells])
      InternalRowTile.constantTileFromBytes(data, cellType, cols, rows).toArrayTile()
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
  // Temporary, until GeoTrellis 1.2
  // See https://github.com/locationtech/geotrellis/pull/2401
  private[InternalRowTile] def constantTileFromBytes(bytes: Array[Byte], t: CellType, cols: Int, rows: Int): ConstantTile =
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
