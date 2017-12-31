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

import astraea.spark.rasterframes.expressions.IntersectsExpression
import astraea.spark.rasterframes.jts.SpatialPredicates
import geotrellis.util.LazyLogging
import org.apache.spark.sql.PointUDT
import org.apache.spark.sql.SQLRules.GeometryLiteral
import org.apache.spark.sql.SQLSpatialFunctions.ST_Contains
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Expression, GetStructField, GreaterThanOrEqual, In, LessThanOrEqual, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

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
  import astraea.spark.rasterframes.encoders.GeoTrellisEncoders._
  object Contains {
    def unapply(in: Expression): Option[(Expression, Expression)] = in match {
      case ScalaUDF(ST_Contains, BooleanType, Seq(left, right), _) ⇒
        Some((left, right))
      case _ ⇒ None
    }
  }

  object ExtentRel {
    def unapply(in: AttributeReference): Option[(Expression, Expression, Expression, Expression)] = {
      in.dataType match {
        case t if t == extentEncoder.schema ⇒
//          Some((
//            GetStructField(in, 0, Some("xmin")),
//            GetStructField(in, 1, Some("ymin")),
//            GetStructField(in, 2, Some("xmax")),
//            GetStructField(in, 3, Some("ymax"))
//          ))
//          val ref = s"${in.name}#${in.exprId.id}"
//          Some((
//            AttributeReference(s"$ref.xmin", DoubleType, false)(),
//            AttributeReference(s"$ref.ymin", DoubleType, false)(),
//            AttributeReference(s"$ref.xmax", DoubleType, false)(),
//            AttributeReference(s"$ref.ymax", DoubleType, false)()
//          ))
//          val ref = Some(s"${in.name}")
          Some((
            AttributeReference("xmin", DoubleType, false)(),
            AttributeReference("ymin", DoubleType, false)(),
            AttributeReference("xmax", DoubleType, false)(),
            AttributeReference("ymax", DoubleType, false)()
          ))

          //val ref = s"${in.name}"
//          Some((
//            UnresolvedAttribute(s"${in.name}.xmin"),
//            UnresolvedAttribute(s"${in.name}.ymin"),
//            UnresolvedAttribute(s"${in.name}.xmax"),
//            UnresolvedAttribute(s"${in.name}.ymax")
//          ))
//          Some((
//            UnresolvedExtractValue(in, Literal("xmin")),
//            UnresolvedExtractValue(in, Literal("ymin")),
//            UnresolvedExtractValue(in, Literal("xmax")),
//            UnresolvedExtractValue(in, Literal("ymax"))
//          ))

        case _ ⇒ None
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case f @ Filter(IntersectsExpression(
      e @ ExtentRel(xmin, ymin, xmax, ymax), g: GeometryLiteral), child) ⇒
        val tmp = g.geom.getCentroid

        val resolver: Resolver = _ == _
        val Some(_xmin) = plan.resolve(Seq("extent", "xmin"), resolver)
        val Some(_ymin) = plan.resolve(Seq("extent", "ymin"), resolver)
        val Some(_xmax) = plan.resolve(Seq("extent", "xmax"), resolver)
        val Some(_ymax) = plan.resolve(Seq("extent", "ymax"), resolver)

//        val xmin = Alias(_xmin, "xmin")()
//        val xmax = Alias(_xmax, "xmax")()
//        val ymin = Alias(_ymin, "ymin")()
//        val ymax = Alias(_ymax, "ymax")()

        val cond = And(
          And(LessThanOrEqual(_xmin, Literal(tmp.getX)), LessThanOrEqual(_ymin, Literal(tmp.getY))),
          And(GreaterThanOrEqual(_xmax, Literal(tmp.getX)), GreaterThanOrEqual(_ymax, Literal(tmp.getY)))
        )

        val proj =  Project(Seq(
          AttributeReference("xmin", DoubleType, false)(),
          AttributeReference("ymin", DoubleType, false)(),
          AttributeReference("xmax", DoubleType, false)(),
          AttributeReference("ymax", DoubleType, false)()
        ), child)
//        val cond = In(e, Seq(Literal.fromObject(g.geom, PointUDT)))

        f.copy(condition = cond, child = proj)
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
