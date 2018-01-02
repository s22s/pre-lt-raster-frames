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
import astraea.spark.rasterframes.expressions.SpatialExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.SQLSpatialFunctions._
import org.apache.spark.sql.functions.udf
import astraea.spark.rasterframes.util._
import org.apache.spark.sql.gt._

/**
 * UDF wrappers around JTS spark relations.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialPredicates {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
  def contains(left: Column, right: Column) = withAlias("contains", left, right)(
    udf(ST_Contains).apply(left, right)
  ).as[Boolean]
  def covers(left: Column, right: Column) = withAlias("covers", left, right)(
    udf(ST_Covers).apply(left, right)
  ).as[Boolean]
  def crosses(left: Column, right: Column) = withAlias("crosses", left, right)(
    udf(ST_Crosses).apply(left, right)
  ).as[Boolean]
  def disjoint(left: Column, right: Column) = withAlias("disjoint", left, right)(
    udf(ST_Disjoint).apply(left, right)
  ).as[Boolean]
  def equals(left: Column, right: Column) = withAlias("equals", left, right)(
    udf(ST_Equals).apply(left, right)
  ).as[Boolean]
  def intersects(left: Column, right: Column) = SpatialExpression.Intersects(left.expr, right.expr).asColumn
  //withAlias("intersects", left, right)(
  //  udf(ST_Intersects).apply(left, right)
  //).as[Boolean]
  def overlaps(left: Column, right: Column) = withAlias("overlaps", left, right)(
    udf(ST_Overlaps).apply(left, right)
  ).as[Boolean]
  def touches(left: Column, right: Column) = withAlias("touches", left, right)(
    udf(ST_Touches).apply(left, right)
  ).as[Boolean]
  def within(left: Column, right: Column) = withAlias("within", left, right)(
    udf(ST_Within).apply(left, right)
  ).as[Boolean]
}

object SpatialPredicates {

}
