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

import astraea.spark.rasterframes
import astraea.spark.rasterframes.sources.catalog.L8CatalogDataSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 * @author sfitch 
 * @since 9/28/17
 */
object DataSourceFun extends App {

  implicit val spark = SparkSession.builder().
    master("local[*]").appName("RasterFrames").getOrCreate()

  rasterframes.rfInit(spark.sqlContext)
  import spark.implicits._

  val catalog = spark.read.format(L8CatalogDataSource.NAME).load()

  catalog.filter($"path" === 15 && $"row" === 34).orderBy(asc("cloudCover")).show
}




