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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.gt.types

/**
 * Object responsible for registering functions with Catalyst
 *
 * @author sfitch
 * @since 4/12/17
 */
private[rasterframes] object Registrator {
  def register(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rf_makeConstantTile", UDFs.makeConstantTile)
    sqlContext.udf.register("rf_tileDimensions", UDFs.tileDimensions)
    sqlContext.udf.register("rf_cellType", UDFs.cellType)
    sqlContext.udf.register("rf_tileToArrayInt", UDFs.tileToArray[Int])
    sqlContext.udf.register("rf_tileToArrayDouble", UDFs.tileToArray[Double])
    sqlContext.udf.register("rf_histogram", UDFs.aggHistogram)
    sqlContext.udf.register("rf_stats", UDFs.aggStats)
    sqlContext.udf.register("rf_tileMean", UDFs.tileMean)
    sqlContext.udf.register("rf_tileHistogram", UDFs.tileHistogram)
    sqlContext.udf.register("rf_tileStats", UDFs.tileStats)
    sqlContext.udf.register("rf_dataCells", UDFs.dataCells)
    sqlContext.udf.register("rf_nodataCells", UDFs.dataCells)
    sqlContext.udf.register("rf_tileMeanDouble", UDFs.tileMeanDouble)
    sqlContext.udf.register("rf_tileHistogramDouble", UDFs.tileHistogramDouble)
    sqlContext.udf.register("rf_tileStatsDouble", UDFs.tileStatsDouble)
    sqlContext.udf.register("rf_localAggStats", UDFs.localAggStats)
    sqlContext.udf.register("rf_localAggMax", UDFs.localAggMax)
    sqlContext.udf.register("rf_localAggMin", UDFs.localAggMin)
    sqlContext.udf.register("rf_localAggMean", UDFs.localAggMean)
    sqlContext.udf.register("rf_localAggCount", UDFs.localAggCount)
    sqlContext.udf.register("rf_localAdd", UDFs.localAdd)
    sqlContext.udf.register("rf_localSubtract", UDFs.localSubtract)
    sqlContext.udf.register("rf_localMultiply", UDFs.localMultiply)
    sqlContext.udf.register("rf_localDivide", UDFs.localDivide)
    sqlContext.udf.register("rf_cellTypes", types.cellTypes)
    sqlContext.udf.register("rf_renderAscii", UDFs.renderAscii)
  }
  // Expression-oriented functions have a different registration scheme
  // Currently have to register with the `builtin` registry due to Spark data hiding.
  FunctionRegistry.builtin.registerFunction("rf_explodeTiles", ExplodeTileExpression.apply(1.0, _))
}
