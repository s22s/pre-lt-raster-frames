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

import astraea.spark.rasterframes.encoders.StandardEncoders._
import astraea.spark.rasterframes.expressions.SpatialExpression.RelationPredicate
import astraea.spark.rasterframes.jts.SpatialEncoders._
import com.vividsolutions.jts.geom._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.sql.jts._
import org.locationtech.geomesa.spark.SQLSpatialFunctions


/**
 * Determine if two spatial constructs intersect each other.
 *
 * @author sfitch 
 * @since 12/28/17
 */
abstract class SpatialExpression extends BinaryExpression with CodegenFallback {

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = left.nullable || right.nullable

  private def asGeom(expr: Expression, item: Any): Geometry = {
    item match {
      case g: Geometry ⇒ g
      case r: InternalRow ⇒
        expr.dataType match {
          case t if t == extentEncoder.schema ⇒
            val extent = extentEncoder.resolveAndBind().fromRow(r)
            extent.jtsGeom
          case t if t == jtsPointEncoder.schema ⇒
            jtsPointEncoder.resolveAndBind().fromRow(r)
          case t if t.getClass.isAssignableFrom(PointUDT.getClass) ⇒
            PointUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(MultiPointUDT.getClass) ⇒
            MultiPointUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(LineStringUDT.getClass) ⇒
            LineStringUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(MultiLineStringUDT.getClass) ⇒
            MultiLineStringUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(PolygonUDT.getClass) ⇒
            PolygonUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(MultiPolygonUDT.getClass) ⇒
            MultiPolygonUDT.deserialize(r)
          case t if t.getClass.isAssignableFrom(GeometryUDT.getClass) ⇒
            GeometryUDT.deserialize(r)
        }
    }
  }

  override protected def nullSafeEval(leftEval: Any, rightEval: Any): java.lang.Boolean = {
    val leftGeom = asGeom(left, leftEval)
    val rightGeom = asGeom(right, rightEval)
    relation(leftGeom, rightGeom)
  }

  val relation: RelationPredicate

}

object SpatialExpression {
  type RelationPredicate = (Geometry, Geometry) ⇒ java.lang.Boolean

  case class Intersects(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "intersects"
    val relation = SQLSpatialFunctions.ST_Intersects
  }
  case class Contains(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "contains"
    val relation = SQLSpatialFunctions.ST_Contains
  }
  case class Covers(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "covers"
    val relation = SQLSpatialFunctions.ST_Covers
  }
  case class Crosses(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "crosses"
    val relation = SQLSpatialFunctions.ST_Crosses
  }
  case class Disjoint(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "disjoint"
    val relation = SQLSpatialFunctions.ST_Disjoint
  }
  case class Overlaps(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "overlaps"
    val relation = SQLSpatialFunctions.ST_Overlaps
  }
  case class Touches(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "touches"
    val relation = SQLSpatialFunctions.ST_Touches
  }
  case class Within(left: Expression, right: Expression) extends SpatialExpression {
    override def nodeName = "within"
    val relation = SQLSpatialFunctions.ST_Within
  }
}
