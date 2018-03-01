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

/**
 * Tests type encoding for spatial types.
 *
 * @since 12/17/17
 */
class SpatialEncodingSpec extends TestEnvironment with TestData with IntelliJPresentationCompilerHack {

  import sqlContext.implicits._

  describe("Dataframe encoding ops on spatial types") {

    it("should code RDD[Point]") {
      val points = Seq(null, extent.center.jtsGeom, null)
      val ds = points.toDS
      write(ds)
      ds.show()

      assert(ds.collect().toSeq === points)
    }
  }
}
