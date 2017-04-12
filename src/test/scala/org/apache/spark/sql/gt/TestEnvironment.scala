package org.apache.spark.sql.gt

import geotrellis.spark.testkit.{TestEnvironment => GeoTrellisTestEnvironment}

import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest._

import scala.util.Properties

trait TestEnvironment extends GeoTrellisTestEnvironment { self: Suite with BeforeAndAfterAll ⇒
  val _spark: SparkSession = {
    System.setProperty("spark.driver.port", "0")
    System.setProperty("spark.hostPort", "0")
    System.setProperty("spark.ui.enabled", "false")

    val session = SparkSession.builder()
      .master("local")
      .appName("Test Context")
      .getOrCreate()

    // Shortcut out of using Kryo serialization if we want to test against
    // java serialization.
    if(Properties.envOrNone("GEOTRELLIS_USE_JAVA_SER").isEmpty) {
      val conf = session.sparkContext.getConf
      conf
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryoserializer.buffer.max", "500m")
        .set("spark.kryo.registrationRequired", "false")
      setKryoRegistrator(conf)
    }

    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    System.clearProperty("spark.ui.enabled")

    session
  }

  override implicit def sc: SparkContext = _spark.sparkContext

  lazy val sql: SQLContext = _spark.sqlContext
}
