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


import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import com.vividsolutions.jts.{geom ⇒ jts}

/**
 * Encoder implicits for JTS UDTs.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialEncoders {
  implicit def jtsGeometryEncoder = ExpressionEncoder[jts.Geometry]()
  implicit def jtsPointEncoder = ExpressionEncoder[jts.Point]()
  implicit def jtsLineStringEncoder = ExpressionEncoder[jts.LineString]()
  implicit def jtsPolygonEncoder = ExpressionEncoder[jts.Polygon]()
  implicit def jtsMultiPointEncoder = ExpressionEncoder[jts.MultiPoint]()
  implicit def jtsMultiLineStringEncoder = ExpressionEncoder[jts.MultiLineString]()
  implicit def jtsMultiPolygonEncoder = ExpressionEncoder[jts.MultiPolygon]()
  implicit def jtsGeometryCollectionEncoder = ExpressionEncoder[jts.GeometryCollection]()
}
object SpatialEncoders extends SpatialEncoders
