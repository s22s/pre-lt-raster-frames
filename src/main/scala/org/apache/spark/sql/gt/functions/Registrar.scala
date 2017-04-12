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
