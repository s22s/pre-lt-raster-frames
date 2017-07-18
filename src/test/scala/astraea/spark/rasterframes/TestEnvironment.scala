package astraea.spark.rasterframes

import geotrellis.spark.testkit.{TestEnvironment ⇒ GeoTrellisTestEnvironment}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalactic.Tolerance
import org.scalatest._

trait TestEnvironment extends FunSpec with GeoTrellisTestEnvironment
  with Matchers with Inspectors with Tolerance {

  override implicit def sc: SparkContext = _sc
  lazy val sqlContext = {
    val ctx = SQLContext.getOrCreate(_sc)
    rfInit(ctx)
    ctx
  }
  lazy val sql: (String) ⇒ DataFrame = sqlContext.sql
  implicit lazy val spark = sqlContext.sparkSession
}
