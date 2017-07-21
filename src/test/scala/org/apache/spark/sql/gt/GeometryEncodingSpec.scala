/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */
package org.apache.spark.sql.gt

import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.vector._
import org.apache.spark.rdd.RDD

class GeometryEncodingSpec extends TestEnvironment with TestData {
  import GeometryEncodingSpec._

  gtRegister(sqlContext)

  describe("polygon encoding support") {
    it("should not throw a runtime error") {
      import sqlContext.implicits._
      val polyA = Polygon(Line(Point(0,0), Point(1,0), Point(1,1), Point(0,1), Point(0,0)))
      val polyB = Polygon(List(Point(10,10), Point(11,10), Point(11,11), Point(10,11), Point(10,10)))
      val polyC = Polygon(List(Point(100,100), Point(101,100), Point(101,101), Point(100,101), Point(100,100)))

      val left: RDD[Polygon] = sc.parallelize(Array(polyA, polyB, polyC))
      val polygon = left.toDS
      polygon.show(false)
      assert(polygon.count() === 3)

      val line1 = Line(Point(0,0), Point(5,5))
      val line2 = Line(Point(13,0), Point(33,0))

      val rightline: RDD[Feature[MultiLine, Category]] = sc.parallelize(Seq(Feature(MultiLine(line1, line2), Category(1, "some"))))
      val right = rightline.toDS
      right.show(false)
      assert(right.count() === 1)
    }
  }


  describe("multipolygon encoding support") {
    it("should not throw a runtime error") {
      import sqlContext.implicits._
      val polyA = Polygon(Line(Point(0,0), Point(1,0), Point(1,1), Point(0,1), Point(0,0)))
      val polyB = Polygon(List(Point(10,10), Point(11,10), Point(11,11), Point(10,11), Point(10,10)))
      val polyC = Polygon(List(Point(100,100), Point(101,100), Point(101,101), Point(100,101), Point(100,100)))
      val polyD = Polygon(List(Point(120,120), Point(121,120), Point(121,121), Point(120,121), Point(120,120)))
      val polyE = Polygon(List(Point(130,130), Point(131,130), Point(131,131), Point(130,131), Point(130,130)))

      val left: RDD[MultiPolygon] = sc.parallelize(Array(MultiPolygon(Array(polyA, polyB)),MultiPolygon(Array(polyC, polyD, polyE))))
      val multipolygon = left.toDS
      multipolygon.show(false)
      assert(multipolygon.count() === 2)

      assert(multipolygon.head.polygons.size === 2)
    }
  }


}

object GeometryEncodingSpec {
  case class Category(id: Long, name: String)
}
