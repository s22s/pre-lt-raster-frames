package org.apache.spark.sql.gt

import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.scalactic.Tolerance
import org.scalatest.{FunSpec, Inspectors, Matchers}

class GeometryEncodingSpec extends FunSpec
  with Matchers with Inspectors with Tolerance
  with TestEnvironment with TestData {
  import GeometryEncodingSpec._

  gtRegister(_spark.sqlContext)

  describe("polygon encoding support") {
    it("should not throw a runtime error") {
      import _spark.implicits._
      val polyA = Polygon(Line(Point(0,0), Point(1,0), Point(1,1), Point(0,1), Point(0,0)))
      val polyB = Polygon(List(Point(10,10), Point(11,10), Point(11,11), Point(10,11), Point(10,10)))
      val polyC = Polygon(List(Point(100,100), Point(101,100), Point(101,101), Point(100,101), Point(100,100)))

      val left: RDD[Polygon] = sc.parallelize(Array(polyA, polyB, polyC))
      val polygon = left.toDS
      polygon.show(false)
      println(polygon.first)

      val line1 = Line(Point(0,0), Point(5,5))
      val line2 = Line(Point(13,0), Point(33,0))

      val rightline: RDD[Feature[MultiLine, Category]] = sc.parallelize(Seq(Feature(MultiLine(line1, line2), Category(1, "some"))))
      val right = rightline.toDS
      right.show(false)
      println(right.first)
    }
  }
}

object GeometryEncodingSpec {
  case class Category(id: Long, name: String)
}
