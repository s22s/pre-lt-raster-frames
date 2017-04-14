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
private[gt] object Registrar {
  def register(sqlContext: SQLContext): Unit = {
    import UDFs._
    sqlContext.udf.register("st_makeConstantTile", makeConstantTile)
    sqlContext.udf.register("st_focalSum", focalSum)
    sqlContext.udf.register("st_makeTiles", makeTiles)
    sqlContext.udf.register("st_gridRows", gridRows)
    sqlContext.udf.register("st_gridCols", gridCols)
  }
  // Expression-oriented functions have a different registration scheme
  FunctionRegistry.builtin.registerFunction("st_explodeTile", ExplodeTileExpression.apply)
  FunctionRegistry.builtin.registerFunction("st_flattenExtent", (exprs: Seq[Expression]) ⇒ flattenExpression[Extent](exprs.head))
  FunctionRegistry.builtin.registerFunction("st_flattenProjectedExtent", (exprs: Seq[Expression]) ⇒ flattenExpression[ProjectedExtent](exprs.head))
}
