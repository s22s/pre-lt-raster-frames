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

import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Object responsible for registering functions with Catalyst
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] object Registrator {
  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_makeConstantTile", UDFs.makeConstantTile)
    sqlContext.udf.register("st_focalSum", UDFs.focalSum)
    sqlContext.udf.register("st_makeTiles", UDFs.makeTiles)
    sqlContext.udf.register("st_gridRows", UDFs.gridRows)
    sqlContext.udf.register("st_gridCols", UDFs.gridCols)
    sqlContext.udf.register("st_localMax", UDFs.localMax)
    sqlContext.udf.register("st_localMin", UDFs.localMin)
    sqlContext.udf.register("st_tileMean", UDFs.tileMean)
    sqlContext.udf.register("st_tileHistogram", UDFs.tileHistogram)
    sqlContext.udf.register("st_tileStatistics", UDFs.tileStatistics)
    sqlContext.udf.register("st_histogram", UDFs.histogram)
    sqlContext.udf.register("st_randomTile", UDFs.randomTile)
    sqlContext.udf.register("st_cellTypes", UDFs.cellTypes)
    sqlContext.udf.register("st_renderAscii", UDFs.renderAscii)
  }
  // Expression-oriented functions have a different registration scheme
  // Currently have to register with the `builtin` registry due to data hiding.
  FunctionRegistry.builtin.registerFunction("st_explodeTile", ExplodeTileExpression.apply)
}
