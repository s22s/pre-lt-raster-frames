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

package astraea.spark.rasterframes

import astraea.spark.rasterframes.util._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Point ⇒ gtPoint}

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @since 12/16/17
 */
class JTSSpec extends TestEnvironment with TestData with StandardColumns with IntelliJPresentationCompilerHack {
  import spark.implicits._

  describe("JTS interop") {
    it("should allow joining and filtering of tiles based on points") {
      val rf = l8Sample(1).projectedRaster.toRF(10, 10).withExtent()
      val crs = rf.tileLayerMetadata.widen.crs
      val coords = Seq(
        "one" -> gtPoint(-78.6445222907, 38.3957546898).reproject(LatLng, crs).jtsGeom,
        "two" -> gtPoint(-78.6601240367, 38.3976614324).reproject(LatLng, crs).jtsGeom,
        "three" -> gtPoint( -78.6123381343, 38.4001666769).reproject(LatLng, crs).jtsGeom
      )

      val locs = coords.toDF("id", "point")
      withClue("join with point column") {
        assert(rf.join(locs, st_contains(EXTENT_COLUMN, $"point")).count === coords.length)
        assert(rf.join(locs, st_intersects(EXTENT_COLUMN, $"point")).count === coords.length)
      }

      withClue("point literal") {
        val point = coords.head._2
        assert(rf.filter(st_contains(EXTENT_COLUMN, geomlit(point))).count === 1)
        assert(rf.filter(st_intersects(EXTENT_COLUMN, geomlit(point))).count === 1)
        assert(rf.filter(EXTENT_COLUMN intersects point).count === 1)
        assert(rf.filter(EXTENT_COLUMN intersects gtPoint(point)).count === 1)
        assert(rf.filter(EXTENT_COLUMN containsGeom point).count === 1)
      }

      withClue("exercise predicates") {
        val point = geomlit(coords.head._2)
        assert(rf.filter(st_covers(EXTENT_COLUMN, point)).count === 1)
        assert(rf.filter(st_crosses(EXTENT_COLUMN, point)).count === 0)
        assert(rf.filter(st_disjoint(EXTENT_COLUMN, point)).count === rf.count - 1)
        assert(rf.filter(st_overlaps(EXTENT_COLUMN, point)).count === 0)
        assert(rf.filter(st_touches(EXTENT_COLUMN, point)).count === 0)
        assert(rf.filter(st_within(EXTENT_COLUMN, point)).count === 0)
      }
    }

    it("should allow construction of geometry literals") {
      val fact = new GeometryFactory()
      val c1 = new Coordinate(1, 2)
      val c2 = new Coordinate(3, 4)
      val c3 = new Coordinate(5, 6)
      val point = fact.createPoint(c1)
      val line = fact.createLineString(Array(c1, c2))
      val poly = fact.createPolygon(Array(c1, c2, c3, c1))
      val mpoint = fact.createMultiPoint(Array(point, point, point))
      val mline = fact.createMultiLineString(Array(line, line, line))
      val mpoly = fact.createMultiPolygon(Array(poly, poly, poly))
      val coll = fact.createGeometryCollection(Array(point, line, poly, mpoint, mline, mpoly))
      assert(dfBlank.select(geomlit(point)).first === point)
      assert(dfBlank.select(geomlit(line)).first === line)
      assert(dfBlank.select(geomlit(poly)).first === poly)
      assert(dfBlank.select(geomlit(mpoint)).first === mpoint)
      assert(dfBlank.select(geomlit(mline)).first === mline)
      assert(dfBlank.select(geomlit(mpoly)).first === mpoly)
      assert(dfBlank.select(geomlit(coll)).first === coll)
    }
  }
}
