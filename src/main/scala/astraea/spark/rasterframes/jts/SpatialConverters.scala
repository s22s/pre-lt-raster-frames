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

import com.vividsolutions.jts.geom.{Geometry, Point, Polygon}
import org.apache.spark.sql.SQLGeometricOutputFunctions._
import org.apache.spark.sql.SQLGeometricConstructorFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import astraea.spark.rasterframes.util._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions.Literal

/**
 * UDFs providing conversion to/from JTS types.
 *
 * @author sfitch 
 * @since 12/17/17
 */
trait SpatialConverters {
  import SpatialEncoders._

  def geomAsWKT(geom: Column) = withAlias("geomAsWKT", geom) {
    udf(ST_AsText).apply(geom)
  }.as[String]
  def geomFromWKT(wkt: Column) = withAlias("geomFromWKT", wkt) {
    udf(ST_GeomFromWKT).apply(wkt)
  }.as[Geometry]
  def geomAsGeoJSON(geom: Column) = withAlias("geomAsGeoJSON", geom) {
    udf(ST_AsGeoJSON).apply(geom)
  }.as[String]
  def pointFromWKT(wkt: Column) = withAlias("pointFromWKT", wkt) {
    udf(ST_PointFromText).apply(wkt)
  }.as[Point]
  def pointFromWKT(wkt: String) = {
    udf(() ⇒ ST_PointFromText(wkt)).apply().as("pointFromWKT")
  }.as[Point]
  def makePoint(x: Double, y: Double) = {
    udf(() ⇒ ST_MakePoint(x, y)).apply().as("makePoint()")
  }.as[Point]
  def makePoint(x: Column, y: Column) = withAlias("makePoint", x, y) {
    udf(ST_MakePoint).apply(x, y)
  }.as[Point]
  def makeBBox(lowerX: Column, lowerY: Column, upperX: Column, upperY: Column) =
    withAlias("makeBBox", lowerX, lowerY, upperX, upperY)(
      udf(ST_MakeBBOX).apply(lowerX, lowerY, upperX, upperY)
    ).as[Geometry]
  def makeBBox(lowerX: Double, lowerY: Double, upperX: Double, upperY: Double) = {
    udf(() ⇒ ST_MakeBBOX(lowerX, lowerY, upperX, upperY)).apply().as("makeBBox()")
  }.as[Geometry]

  @Experimental
  def geomlit(geom: Geometry) = geom match {
    case g: Point ⇒ new Column(Literal(g, PointUDT.sqlType))
    case g: Polygon ⇒ new Column(Literal(g, PolygonUDT.sqlType))
  }
}
