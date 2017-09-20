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

package examples

import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.render._
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Clustering extends App {

  def readTiff(name: String): SinglebandGeoTiff =
    SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream(s"/$name")))

  implicit val spark = SparkSession.builder().master("local[*]").appName(getClass.getName).getOrCreate()

  rfInit(spark.sqlContext)
  import spark.implicits._


  val filenamePattern = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 4

  val joinedRF = bandNumbers
    .map { b ⇒ (b, filenamePattern.format(b)) }
    .map { case (b,f) ⇒ (b, readTiff(f)) }
    .map { case (b, t) ⇒ t.projectedRaster.toRF(s"B$b") }
    .reduce(_ spatialJoin _)

  joinedRF.printSchema()

  spark.stop()
}
