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

package astraea.spark.rasterframes

import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.util.LazyLogging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rf._
import org.apache.spark.sql.{Column, SQLContext}


/**
 * Internal utilities.
 *
 * @author sfitch
 * @since 12/18/17
 */
package object util extends LazyLogging {
  /** Tags output column with a nicer name. */
  private[rasterframes]
  def withAlias(name: String, inputs: Column*)(output: Column) = {
    val paramNames = inputs.map(_.columnName).mkString(",")
    output.as(s"$name($paramNames)")
  }

  /** Derives and operator name from the implementing object name. */
  private[rasterframes]
  def opName(op: LocalTileBinaryOp) =
    op.getClass.getSimpleName.replace("$", "").toLowerCase


  // $COVERAGE-OFF$
  private[rasterframes]
  implicit class Pipeable[A](val a: A) extends AnyVal {
    def |>[B](f: A ⇒ B): B = f(a)
  }

  /** Applies the given thunk to the closable resource. */
  def withResource[T <: CloseLike, R](t: T)(thunk: T ⇒ R): R = {
    import scala.language.reflectiveCalls
    try { thunk(t) } finally { t.close() }
  }

  /** Anything that structurally has a close method. */
  type CloseLike = { def close(): Unit }

  implicit class Conditionalize[T](left: T) {
    def when(pred: T ⇒ Boolean): Option[T] = Option(left).filter(pred)
  }

  private[rasterframes]
  def toParquetFriendlyColumnName(name: String) = name.replaceAll("[ ,;{}()\n\t=]", "_")

  def registerResolution(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    logger.error("Extended rule resolution not available in this version of Spark")
    analyzer(sqlContext).extendedResolutionRules
  }
  // $COVERAGE-ON$
}
