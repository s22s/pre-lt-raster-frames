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

package astraea.spark.rasterframes.datasource

import astraea.spark.rasterframes.jts.SpatialPredicates
import geotrellis.util.LazyLogging
import org.apache.spark.sql.SQLRules.GeometryLiteral
import org.apache.spark.sql.SQLSpatialFunctions.ST_Contains
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.BooleanType

/**
 * Logical plan manipulations to handle spatial queries on tile components.
 * Starting points for understanding the Catalyst optimizer:
 * - [[org.apache.spark.sql.catalyst.trees.TreeNode]]
 * - [[org.apache.spark.sql.catalyst.expressions.Expression]]
 * - [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]
 * - [[org.apache.spark.sql.catalyst.analysis.Analyzer]]
 * - [[org.apache.spark.sql.catalyst.optimizer.Optimizer]]
 *
 * @author sfitch 
 * @since 12/21/17
 */
object SpatialFilterPushdownRules extends Rule[LogicalPlan] with LazyLogging {

  object Contains {
    def unapply(in: Expression): Option[(Expression, Expression)] = in match {
      case ScalaUDF(ST_Contains, BooleanType, Seq(left, right), _) ⇒
        Some((left, right))
      case _ ⇒ None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    logger.debug(s"Evaluating $plan")
    plan.transform {
      case f @ Filter(Contains(left, GeometryLiteral(_, geom)),
      LogicalRelation(gm: GeoTrellisRelation, _, _)) ⇒
        logger.debug("left: " + left)
        logger.debug("right: " + geom)
        f
      case f @ Filter(ScalaUDF(fcn, BooleanType, children, types),
      LogicalRelation(gm: GeoTrellisRelation, _, _)) ⇒
        logger.debug("udf: " + fcn.getClass)
        logger.debug("expression: " + children)
        logger.debug("types: " + types)
        logger.debug("relation: " + gm)
        f
    }
  }
}
