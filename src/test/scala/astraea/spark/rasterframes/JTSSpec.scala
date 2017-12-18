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

import astraea.spark.rasterframes.encoders.PointEncoder
import geotrellis.proj4.LatLng
import geotrellis.vector.Point
import org.apache.spark.sql.Encoders

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @author sfitch
 * @since 12/16/17
 */
class JTSSpec extends TestEnvironment with TestData with IntelliJPresentationCompilerHack {
  import spark.implicits._

  describe("JTS interop") {
    it("should allow filtering of tiles based on points") {
      // TODO: Figure out how to get rid of this when Points are within other structures.
      // Hoping UDFs aren't required.
      implicit def pairEncoder = Encoders.tuple(Encoders.STRING, PointEncoder())
      val rf = l8Sample(1).projectedRaster.toRF(10, 10).withBounds()
      val crs = rf.tileLayerMetadata.widen.crs
      val coords = Seq(
        "one" -> Point(-78.6445222907, 38.3957546898).reproject(LatLng, crs),
        "two" -> Point(-78.6601240367, 38.3976614324).reproject(LatLng, crs),
        "three" -> Point( -78.6123381343, 38.4001666769).reproject(LatLng, crs)
      )

      val locs = coords.toDF("id", "point")

      //rf.show(false)
      rf.printSchema()
      locs.printSchema()

      rf.drop($"tile").join(locs, contains($"bounds", $"point")).show(false)
    }

  }
}
