package org.apache.spark.sql.gt

import geotrellis.spark.testkit.{TestEnvironment ⇒ GeoTrellisTestEnvironment}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest._

trait TestEnvironment extends GeoTrellisTestEnvironment { self: Suite with BeforeAndAfterAll ⇒

  override implicit def sc: SparkContext = _sc
  lazy val sqlContext = SQLContext.getOrCreate(_sc)

  lazy val sql: (String) ⇒ DataFrame = sqlContext.sql
}
