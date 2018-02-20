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

import geotrellis.proj4.LatLng
import geotrellis.vector.Point
import org.locationtech.geomesa.spark.jts._
import astraea.spark.rasterframes.util._

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @since 12/16/17
 */
class JTSSpec extends TestEnvironment with TestData with IntelliJPresentationCompilerHack {
  import spark.implicits._

  describe("JTS interop") {
    it("should allow joining and filtering of tiles based on points") {
      val rf = l8Sample(1).projectedRaster.toRF(10, 10).withExtent()
      val crs = rf.tileLayerMetadata.widen.crs
      val coords = Seq(
        "one" -> Point(-78.6445222907, 38.3957546898).reproject(LatLng, crs).jtsGeom,
        "two" -> Point(-78.6601240367, 38.3976614324).reproject(LatLng, crs).jtsGeom,
        "three" -> Point( -78.6123381343, 38.4001666769).reproject(LatLng, crs).jtsGeom
      )

      val locs = coords.toDF("id", "point")
      val joined = rf.join(locs, st_contains(EXTENT_COLUMN, $"point"))
      assert(joined.count === coords.length)

      val searchPoint = st_pointFromText(coords.head._2.toText)
      val filtered = rf.filter(st_contains(EXTENT_COLUMN, searchPoint))
      assert(filtered.count === 1)
    }
  }
}
