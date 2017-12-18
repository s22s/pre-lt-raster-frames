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

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SQLGeometricOutputFunctions._
import org.apache.spark.sql.SQLGeometricConstructorFunctions._
import org.apache.spark.sql.{WKBUtils, WKTUtils}
import org.apache.spark.sql.functions.udf

/**
 * UDFs providing conversion to/from JTS types.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialConverters {
  def geomAsWKT = udf((g: Geometry) ⇒ WKTUtils.write(g))
  def geomAsWKB = udf((g: Geometry) ⇒ WKBUtils.write(g))
  def geomAsGeoJSON = udf(ST_AsGeoJSON)
  def pointAsLatLonText = udf(ST_AsLatLonText)
  def geomAsText = udf(ST_AsText)
  def pointFromWKT = udf(ST_PointFromText)
}
