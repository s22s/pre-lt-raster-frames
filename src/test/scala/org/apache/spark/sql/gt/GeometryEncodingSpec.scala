package org.apache.spark.sql.gt

import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.scalactic.Tolerance
import org.scalatest.{FunSpec, Inspectors, Matchers}

class GeometryEncodingSpec extends FunSpec
  with Matchers with Inspectors with Tolerance
  with TestEnvironment with TestData {
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
