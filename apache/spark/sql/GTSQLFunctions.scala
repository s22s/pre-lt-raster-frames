/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */

package org.apache.spark.sql

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
import org.apache.spark.sql.GTSQLTypes.TileUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * GT functions adapted for Spark SQL use.
 *
 * @author sfitch 
 * @since 4/3/17
 */
object GTSQLFunctions {

  def explodeTile(col: Column) = Column(ExplodeTile(col.expr))

  // -- Private APIs below --

  private[spark] def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_makeConstantTile", makeConstantTile)
    sqlContext.udf.register("st_focalSum", focalSum)
    sqlContext.udf.register("st_makeTiles", makeTiles)
  }
  // Expression-oriented functions have a different registration scheme
  FunctionRegistry.builtin.registerFunction("st_explodeTile", ExplodeTile.apply)

  private[spark] case class ExplodeTile(child: Expression)
    extends UnaryExpression with Generator with CodegenFallback with Serializable {

    override def elementSchema: StructType = StructType(Array(
      StructField("col", DoubleType, false)
    ))

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
      // Expect CCE?
      val tileRow = child.eval(input).asInstanceOf[InternalRow]
      val tile = TileUDT.deserialize(tileRow)
      val rows = new Array[InternalRow](tile.size)
      var i = 0
      tile.foreachDouble((value) ⇒ {rows(i) = InternalRow(value); i += 1})
      rows
    }
  }

  private[spark] object ExplodeTile {
    // Only looks at first item in seq.
    def apply(inputs: Seq[Expression]): ExplodeTile = new ExplodeTile(inputs.head)
  }

  // Constructor for constant tiles
  private[spark] val makeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
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

  private[spark] val makeTiles: (Int) ⇒ Array[Tile] = (count) ⇒
    Array.fill(count)(makeConstantTile(0, 4, 4, "int8raw"))

  // Perform a focal sum over square area with given half/width extent (value of 1 would be a 3x3 tile)
  private[spark] val focalSum: (Tile, Int) ⇒ Tile = (tile, extent) ⇒ Sum(tile, Square(extent))


}
