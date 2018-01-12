/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

import java.sql.Timestamp
import java.time.ZonedDateTime

import com.vividsolutions.jts.geom.{Geometry ⇒ jtsGeometry, Point ⇒ jtsPoint}
import geotrellis.util.MethodExtensions
import geotrellis.vector.{Extent, Point ⇒ gtPoint}
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.functions._



/**
 * Extension methods on typed columns allowing for DSL-like queries over JTS types.
 * @author sfitch 
 * @since 1/10/18
 */
trait Implicits {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._

  implicit class ExtentColumnMethods(val self: TypedColumn[Any, Extent])
    extends MethodExtensions[TypedColumn[Any, Extent]] {

    def intersects(geom: jtsGeometry): TypedColumn[Any, Boolean] =
      SpatialPredicates.intersects(self, SpatialConverters.geomlit(geom))

    def contains(geom: jtsGeometry): TypedColumn[Any, Boolean] =
      SpatialPredicates.contains(self, SpatialConverters.geomlit(geom))

    def intersects(pt: gtPoint): TypedColumn[Any, Boolean] =
      SpatialPredicates.intersects(self, SpatialConverters.geomlit(pt.jtsGeom))
  }

  implicit class PointColumnMethods(val self: TypedColumn[Any, jtsPoint])
    extends MethodExtensions[TypedColumn[Any, jtsPoint]] {

    def intersects(geom: jtsGeometry): TypedColumn[Any, Boolean] =
      SpatialPredicates.intersects(self, SpatialConverters.geomlit(geom))
  }

  implicit class TimestampColumnMethods(val self: TypedColumn[Any, Timestamp])
    extends MethodExtensions[TypedColumn[Any, Timestamp]] {

    import scala.language.implicitConversions
    private implicit def zdt2ts(time: ZonedDateTime): Timestamp =
      new Timestamp(time.toInstant.toEpochMilli)

    def betweenTimes(start: Timestamp, end: Timestamp): TypedColumn[Any, Boolean] =
      self.between(lit(start), lit(end)).as[Boolean]

    def betweenTimes(start: ZonedDateTime, end: ZonedDateTime): TypedColumn[Any, Boolean] =
      betweenTimes(start: Timestamp, end: Timestamp)

    def at(time: Timestamp): TypedColumn[Any, Boolean] = (self === lit(time)).as[Boolean]
    def at(time: ZonedDateTime): TypedColumn[Any, Boolean] = at(time: Timestamp)
  }
}

object Implicits extends Implicits
