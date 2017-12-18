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
import org.apache.spark.sql.SQLSpatialFunctions._
import org.apache.spark.sql.functions.udf


/**
 * UDF wrappers around JTS spark relations.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialRelations {
  def contains = udf(ST_Contains)
  def covers = udf(ST_Covers)
  def crosses = udf(ST_Crosses)
  def disjoint = udf(ST_Disjoint)
  def equals = udf(ST_Equals)
  def intersects = udf(ST_Intersects)
  def overlaps = udf(ST_Overlaps)
  def touches = udf(ST_Touches)
  def within = udf(ST_Within)
}
