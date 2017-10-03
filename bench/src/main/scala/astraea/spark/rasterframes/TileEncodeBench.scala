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

import geotrellis.raster.{ArrayTile, CellType, NODATA, Tile, isNoData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
class TileEncodeBench {

  @transient
  val spark = SparkSession.builder.master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .getOrCreate

  rfInit(spark.sqlContext)

  val tileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  val boundEncoder = tileEncoder.resolveAndBind()

  @Param(Array("uint8", "int32", "float32", "float64"))
  var cellTypeName: String = _

  @Param(Array("128", "256", "512"))
  var tileSize: Int = _

  @transient
  var tile: Tile = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    tile = TileEncodeBench.randomTile(tileSize, tileSize, cellTypeName)
  }

  @Benchmark
  def encode(): InternalRow = {
    tileEncoder.toRow(tile)
  }

  @Benchmark
  def roundTrip(): Tile = {
    val row = tileEncoder.toRow(tile)
    boundEncoder.fromRow(row)
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    spark.stop()
  }
}

object TileEncodeBench {
  val rnd = new scala.util.Random(42)

  /** Construct a tile of given size and cell type populated with random values. */
  def randomTile(cols: Int, rows: Int, cellTypeName: String): Tile = {
    val cellType = CellType.fromName(cellTypeName)
    val tile = ArrayTile.alloc(cellType, cols, rows)
    if(cellType.isFloatingPoint) {
      tile.mapDouble(_ ⇒ rnd.nextGaussian())
    }
    else {
      tile.map(_ ⇒ {
        var c = NODATA
        do {
          c = rnd.nextInt(255)
        } while(isNoData(c))
        c
      })
    }
  }
}
