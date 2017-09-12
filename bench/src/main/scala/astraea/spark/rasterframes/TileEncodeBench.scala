/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea
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

import geotrellis.raster.Tile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import geotrellis.spark._

@BenchmarkMode(Array(Mode.AverageTime))
@Threads(1)
@State(Scope.Benchmark)
class TileEncodeBench {

  @transient
  val spark = SparkSession.builder.master("local[2]")
    .appName(getClass.getSimpleName)
    //.config("spark.ui.enabled", "false")
    .getOrCreate
  rfInit(spark.sqlContext)

  @Param(Array("int32", "float32"))
  var cellTypeName: String = _

  @Param(Array("8", "64", "128", "256"))
  var tileSize: Int = _

  @Param(Array("2048"))
  var rasterSize: Int = _

  @transient
  var rdd: RDD[Tile] = _

  @Setup(Level.Trial)
  def setupData(): Unit ={
    rdd = spark.sparkContext.makeRDD(TileEncodeBench.makeData(rasterSize, tileSize, cellTypeName), 2)
    rdd.count()
  }

  @Benchmark
  def encode() = {
    // TODO: Figure out a way to run this without a spark job to better benchmark just the encoding part.
    val s = spark
    import s.implicits._
    rdd.toDF.count()
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    spark.stop()
  }
}

object TileEncodeBench {
  def makeData(rasterSize: Int, tileSize: Int, cellTypeName: String) = {
    val raster = TestData.randomTile(rasterSize, rasterSize, cellTypeName)
    raster.split(tileSize, tileSize).toSeq
  }
}
