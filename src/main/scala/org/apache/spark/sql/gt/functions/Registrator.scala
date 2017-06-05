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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

/**
 * Object responsible for registering functions with Catalyst
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] object Registrator {
  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_makeConstantTile", UDFs.makeConstantTile)
    sqlContext.udf.register("st_tileDimensions", UDFs.tileDimensions)
    sqlContext.udf.register("st_histogram", UDFs.aggHistogram)
    sqlContext.udf.register("st_stats", UDFs.aggStats)
    sqlContext.udf.register("st_tileMean", UDFs.tileMean)
    sqlContext.udf.register("st_tileHistogram", UDFs.tileHistogram)
    sqlContext.udf.register("st_tileStats", UDFs.tileStats)
    sqlContext.udf.register("st_tileMeanDouble", UDFs.tileMeanDouble)
    sqlContext.udf.register("st_tileHistogramDouble", UDFs.tileHistogramDouble)
    sqlContext.udf.register("st_tileStatsDouble", UDFs.tileStatsDouble)
    sqlContext.udf.register("st_localStats", UDFs.localStats)
    sqlContext.udf.register("st_localMax", UDFs.localMax)
    sqlContext.udf.register("st_localMin", UDFs.localMin)
    sqlContext.udf.register("st_localAdd", UDFs.localAdd)
    sqlContext.udf.register("st_localSubtract", UDFs.localSubtract)
    sqlContext.udf.register("st_randomTile", UDFs.randomTile)
    sqlContext.udf.register("st_cellTypes", UDFs.cellTypes)
    sqlContext.udf.register("st_renderAscii", UDFs.renderAscii)
    sqlContext.udf.register("st_makeTiles", UDFs.makeTiles)
  }
  // Expression-oriented functions have a different registration scheme
  // Currently have to register with the `builtin` registry due to data hiding.
  FunctionRegistry.builtin.registerFunction("st_explodeTiles", ExplodeTileExpression.apply(1.0, _))
  FunctionRegistry.builtin.registerFunction("st_vectorizeTiles", VectorizeTilesExpression.apply(1.0, true, _))
}
