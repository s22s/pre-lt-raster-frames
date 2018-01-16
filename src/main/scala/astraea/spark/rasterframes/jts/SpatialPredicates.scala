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

package astraea.spark.rasterframes.jts
import astraea.spark.rasterframes.expressions.SpatialExpression._
import astraea.spark.rasterframes.util._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.gt._
import org.locationtech.geomesa.spark.SQLSpatialFunctions._


/**
 * UDF wrappers around JTS spark relations.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialPredicates {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
  def intersects(left: Column, right: Column) =
    Intersects(left.expr, right.expr).asColumn.as[Boolean]
  def contains(left: Column, right: Column) =
    Contains(left.expr, right.expr).asColumn.as[Boolean]
  def covers(left: Column, right: Column) =
    Covers(left.expr, right.expr).asColumn.as[Boolean]
  def crosses(left: Column, right: Column) =
    Crosses(left.expr, right.expr).asColumn.as[Boolean]
  def disjoint(left: Column, right: Column) =
    Disjoint(left.expr, right.expr).asColumn.as[Boolean]
  def overlaps(left: Column, right: Column) =
    Overlaps(left.expr, right.expr).asColumn.as[Boolean]
  def touches(left: Column, right: Column) =
    Touches(left.expr, right.expr).asColumn.as[Boolean]
  def within(left: Column, right: Column) =
    Within(left.expr, right.expr).asColumn.as[Boolean]


//  def equals(left: Column, right: Column) = withAlias("equals", left, right)(
//    udf(ST_Equals).apply(left, right)
//  ).as[Boolean]
}

object SpatialPredicates extends SpatialPredicates
