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

package astraea.spark.rasterframes.ml

import astraea.spark.rasterframes
import astraea.spark.rasterframes.ml.Parameters.HasInputCols
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
 * SparkML Transformer
 *
 * @author sfitch
 * @since 9/21/17
 */
class TileExploder(override val uid: String) extends Transformer
  with HasInputCols {

  def this() = this(Identifiable.randomUID("cell-transformer"))

  final def setInputCols(value: Array[String]) = set(inputCols, value)
  setInputCols(Array("tile"))

  override def copy(extra: ParamMap) = defaultCopy(extra)

  def nonTileFields(schema: StructType): Array[StructField] = {
    schema.fields
      .filter(!_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
  }

  /** Checks the incoming schema and determines what the output schema will be. */
  def transformSchema(schema: StructType) = {
    // Non-tile fields we pass on. Selected tile fields will be changed to doubles
    // other tile fields are dropped.
    val nonTiles = nonTileFields(schema)
    val cells = for {
      colName ← getInputCols
      field ← {
        val f = schema.fields.find(_.name == colName)
        if (f.isEmpty)
          throw new IllegalArgumentException(s"Output column $colName not found in $schema.")
        f
      }
    } yield field.copy(dataType = DoubleType)

    StructType(nonTiles ++ cells)
  }

  def transform(dataset: Dataset[_]) = {
    // Need to preserve all columns that are not tiles.
    val nonTilesToKeep = nonTileFields(dataset.schema)
      .map(f ⇒ col(f.name))

    val exploder = rasterframes.explodeTiles(getInputCols.map(col): _*)

    dataset.select(nonTilesToKeep :+ exploder: _*)
  }
}
