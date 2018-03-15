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
package astraea.spark.rasterframes

import java.nio.file.{Files, Paths}

import astraea.spark.rasterframes.util.toParquetFriendlyColumnName
import geotrellis.spark.testkit.{TestEnvironment ⇒ GeoTrellisTestEnvironment}
import geotrellis.util.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalactic.Tolerance
import org.scalatest._

trait TestEnvironment extends FunSpec with GeoTrellisTestEnvironment
  with Matchers with Inspectors with Tolerance with LazyLogging {

  override implicit def sc: SparkContext = { _sc.setLogLevel("ERROR"); _sc }

  lazy val sqlContext: SQLContext = {
    val session = SparkSession.builder.config(_sc.getConf).getOrCreate()
    astraea.spark.rasterframes.WithSQLContextMethods(session.sqlContext).withRasterFrames
  }

  lazy val sql: (String) ⇒ DataFrame = sqlContext.sql
  implicit val spark = sqlContext.sparkSession

  def isCI: Boolean = sys.env.get("CI").contains("true")

  /** This is here so we can test writing UDF generated/modified GeoTrellis types to ensure they are Parquet compliant. */
  def write(df: Dataset[_]): Boolean = {
    val sanitized = df.select(df.columns.map(c ⇒ col(c).as(toParquetFriendlyColumnName(c))): _*)
    val inRows = sanitized.count()
    val dest = Files.createTempFile(Paths.get(outputLocalPath), "GTSQL", ".parquet")
    logger.debug(s"Writing '${sanitized.columns.mkString(", ")}' to '$dest'...")
    sanitized.write.mode(SaveMode.Overwrite).parquet(dest.toString)
    val rows = df.sparkSession.read.parquet(dest.toString).count()
    logger.debug(s" it has $rows row(s)")
    rows == inRows
  }

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
  def dfBlank(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }
}

/** IntelliJ incorrectly indicates that `withFixture` needs to be implemented, resulting
 * in a distracting error. This for whatever reason gets it to quiet down. */
trait IntelliJPresentationCompilerHack { this: Suite ⇒
  // This is to avoid an IntelliJ error
  protected def withFixture(test: Any) = ???
}
