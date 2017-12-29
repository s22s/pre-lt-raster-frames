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

package astraea.spark.rasterframes.expressions

import astraea.spark.rasterframes.RFSpatialColumnMethods
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import astraea.spark.rasterframes.encoders.GeoTrellisEncoders._
import astraea.spark.rasterframes.jts.SpatialEncoders._
import com.vividsolutions.jts.geom.Geometry

/**
 * Determine if two spatial constructs intersect each other.
 *
 * @author sfitch 
 * @since 12/28/17
 */
case class IntersectsExpression(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def toString: String = s"intersects($left, $right)"

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = left.nullable || right.nullable

  private def asGeom(expr: Expression, item: Any): Geometry = {
    item match {
      case g: Geometry ⇒ g
      case r: InternalRow ⇒
        expr.dataType match {
          case e if e == extentEncoder.schema ⇒
            val extent = extentEncoder.resolveAndBind().fromRow(r)
            extent.jtsGeom
          case e if e == jtsPointEncoder.schema ⇒
            jtsPointEncoder.resolveAndBind().fromRow(r)
        }
    }
  }

  override protected def nullSafeEval(leftEval: Any, rightEval: Any): Any = {
    val leftGeom = asGeom(left, leftEval)
    val rightGeom = asGeom(right, rightEval)
    leftGeom.intersects(rightGeom)
  }
}
