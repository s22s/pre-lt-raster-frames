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

package astraea.spark.rasterframes.functions

import astraea.spark.rasterframes.TestData.randomTile
import astraea.spark.rasterframes.{TestData, TestEnvironment, localAggMax, localAggMin, tileMeanDouble, tileStatsDouble, _}
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.local.{Max, Min}
import geotrellis.raster.summary.Statistics
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import org.apache.spark.sql._

/**
 * Test rig associated with computing statistics and other descriptive
 * information over tiles.
 *
 * @author sfitch 
 * @since 9/18/17
 */
class TileStatsSpec extends TestEnvironment with TestData  {
  import TestData.injectND
  import sqlContext.implicits._
  describe("computing statistics over tiles") {
    it("should report dimensions") {
      val query = sql(
        """|select dims.* from (
           |select st_tileDimensions(tiles) as dims from (
           |select st_makeConstantTile(1, 10, 10, 'int8raw') as tiles))
           |""".stripMargin)
      write(query)
      assert(query.as[(Int, Int)].first() === (10, 10))

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val dims = df.select(tileDimensions($"tile1") as "dims").select("dims.*")
      dims.printSchema()
      dims.show()
      assert(dims.as[(Int, Int)].first() === (3, 3))
      assert(dims.schema.head.name === "cols")
    }

    it("should support local min/max") {
      val ds = Seq[Tile](byteArrayTile, byteConstantTile).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      withClue("max") {
        val max = ds.agg(localAggMax($"tiles"))
        val expected = Max(byteArrayTile, byteConstantTile)
        write(max)
        assert(max.as[Tile].first() === expected)

        val sqlMax = sql("select st_localAggMax(tiles) from tmp")
        assert(sqlMax.as[Tile].first() === expected)

      }

      withClue("min") {
        val min = ds.agg(localAggMin($"tiles"))
        val expected = Min(byteArrayTile, byteConstantTile)
        write(min)
        assert(min.as[Tile].first() === Min(byteArrayTile, byteConstantTile))

        val sqlMin = sql("select st_localAggMin(tiles) from tmp")
        assert(sqlMin.as[Tile].first() === expected)
      }
    }

    it("should compute tile statistics") {
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, "float32")).toDS()
      val means1 = ds.select(tileStatsDouble($"value")).map(_.mean).collect
      val means2 = ds.select(tileMeanDouble($"value")).collect
      assert(means1 === means2)
    }

    it("should compute per-tile histogram") {
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, "float32")).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val r1 = ds.select(tileHistogram($"tiles").as[Histogram[Double]])
      assert(r1.first.totalCount() === 5 * 5)
      write(r1)

      val r2 = sql("select st_tileHistogram(tiles) from tmp")
      write(r2)

      assert(r1.first.mean === r2.as[Histogram[Double]].first.mean)
    }

    it("should compute aggregate histogram") {
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, "float32")).toDF("tiles")
      ds.createOrReplaceTempView("tmp")
      val agg = ds.select(aggHistogram($"tiles")).as[Histogram[Double]]
      val hist = agg.collect()
      assert(hist.length === 1)
      val stats = agg.map(_.statistics().get).as("stats")
      stats.select("stats.*").show(false)
      assert(stats.first().stddev === 1.0 +- 0.3) // <-- playing with statistical fire :)

      val hist2 = sql("select st_histogram(tiles) as hist from tmp").as[Histogram[Double]]

      assert(hist2.first.totalCount() === 250)
    }

    it("should compute aggregate statistics") {
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, "float32")).toDF("tiles")
      ds.createOrReplaceTempView("tmp")
      val agg = ds.select(aggStats($"tiles"))

      assert(agg.first().stddev === 1.0 +- 0.3) // <-- playing with statistical fire :)

      val agg2 = sql("select stats.* from (select st_stats(tiles) as stats from tmp)") .as[Statistics[Double]]
      assert(agg2.first().dataCells === 250)
    }

    def printStatsRows(df: DataFrame): Unit = {
      val tiles = df.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])

      // Render debugging form.
      tiles.map(_.asciiDraw())
        .zip(df.columns)
        .foreach{case (img, label) ⇒ println(s"$label:\n$img")}
    }

    it("should compute aggregate local stats") {
      val ave = (nums: Array[Double]) ⇒ nums.sum / nums.length

      val ds = Seq.fill[Tile](30)(randomTile(5, 5, "float32"))
        .map(injectND(2)).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val agg = ds.select(localAggStats($"tiles") as "stats")
      val stats = agg.select("stats.*")

      printStatsRows(stats)

      val min = agg.select($"stats.min".as[Tile]).map(_.toArrayDouble().min).first
      assert(min < -2.0)
      val max = agg.select($"stats.max".as[Tile]).map(_.toArrayDouble().max).first
      assert(max > 2.0)
      val tendancy = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(tendancy < 0.2)

      val varg = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(varg < 1.1)

      val sqlStats = sql("SELECT stats.* from (SELECT st_localAggStats(tiles) as stats from tmp)")

      val tiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      val dsTiles = sqlStats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      forEvery(tiles.zip(dsTiles)) { case (t1, t2) ⇒
        assert(t1 === t2)
      }
    }

    it("should compute accurate statistics") {
      val tile = squareIncrementingTile(4).convert(IntConstantNoDataCellType)

      val ds = Seq.fill(20)(tile).toDF("tiles")

      val stats = ds.select(localAggStats($"tiles") as "stats").select("stats.*")
      printStatsRows(stats)

      // counted everything properly
      val countTile = ds.select(localAggCount($"tiles")).first()
      forAll(countTile.toArray())(i ⇒ assert(i === 20))

      val meanTile = ds.select(localAggMean($"tiles")).first()
      assert(meanTile.toArray() === tile.toArray())

      val minTile = ds.select(localAggMin($"tiles")).first()
      assert(minTile.toArray() === tile.toArray())

      val maxTile = ds.select(localAggMax($"tiles")).first()
      assert(maxTile.toArray() === tile.toArray())
    }

    it("should count cells by no-data state") {
      val tsize = 5
      val count = 20
      val nds = 2
      val tiles = Seq.fill[Tile](count)(randomTile(tsize, tsize, "uint8ud255"))
        .map(injectND(nds)).toDF("tiles")

      tiles.select(tileStats($"tiles")).show(100)
      val counts = tiles.select(nodataCells($"tiles")).collect()
      forEvery(counts)(c ⇒ assert(c === nds))
      val counts2 = tiles.select(dataCells($"tiles")).collect()
      forEvery(counts2)(c ⇒ assert(c === tsize * tsize - nds))
    }
  }
}