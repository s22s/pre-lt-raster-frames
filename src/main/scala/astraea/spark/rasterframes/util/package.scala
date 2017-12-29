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
import org.apache.spark.sql.{Column, Encoder, SQLContext}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.gt.analyzer

import scala.reflect.runtime.universe._


/**
 * Internal utilities.
 *
 * @author sfitch 
 * @since 12/18/17
 */
package object util {
  private[rasterframes]
  implicit class Pipeable[A](val a: A) extends AnyVal {
    def |>[B](f: A ⇒ B): B = f(a)
  }

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

  private[rasterframes]
  def registerOptimization(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    if(!sqlContext.experimental.extraOptimizations.contains(rule))
      sqlContext.experimental.extraOptimizations :+= rule
  }
  def registerResolution(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    analyzer(sqlContext).extendedResolutionRules
  }
}
