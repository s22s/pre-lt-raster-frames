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

package astraea.spark.rasterframes

import astraea.spark.rasterframes.TestData.randomTile
import astraea.spark.rasterframes.stats.CellHistogram
import geotrellis.raster._
import geotrellis.raster.mapalgebra.local.{Max, Min}
import org.apache.spark.sql.functions._

/**
 * Test rig associated with computing statistics and other descriptive
 * information over tiles.
 *
 * @since 9/18/17
 */
class TileStatsSpec extends TestEnvironment with TestData  {
  import TestData.injectND
  import sqlContext.implicits._
  describe("computing statistics over tiles") {
    //import org.apache.spark.sql.execution.debug._
    it("should report dimensions") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")

      val dims = df.select(tileDimensions($"tile1") as "dims").select("dims.*")

      assert(dims.as[(Int, Int)].first() === (3, 3))
      assert(dims.schema.head.name === "cols")

      val query = sql(
        """|select dims.* from (
           |select rf_tileDimensions(tiles) as dims from (
           |select rf_makeConstantTile(1, 10, 10, 'int8raw') as tiles))
           |""".stripMargin)
      write(query)
      assert(query.as[(Int, Int)].first() === (10, 10))

      df.repartition(4).createOrReplaceTempView("tmp")
      assert(sql("select dims.* from (select rf_tileDimensions(tile2) as dims from tmp)")
        .as[(Int, Int)].first() === (3, 3))
    }

    it("should support local min/max") {
      val ds = Seq[Tile](byteArrayTile, byteConstantTile).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      withClue("max") {
        val max = ds.agg(localAggMax($"tiles"))
        val expected = Max(byteArrayTile, byteConstantTile)
        write(max)
        assert(max.as[Tile].first() === expected)

        val sqlMax = sql("select rf_localAggMax(tiles) from tmp")
        assert(sqlMax.as[Tile].first() === expected)

      }

      withClue("min") {
        val min = ds.agg(localAggMin($"tiles"))
        val expected = Min(byteArrayTile, byteConstantTile)
        write(min)
        assert(min.as[Tile].first() === Min(byteArrayTile, byteConstantTile))

        val sqlMin = sql("select rf_localAggMin(tiles) from tmp")
        assert(sqlMin.as[Tile].first() === expected)
      }
    }

    it("should count data and no-data cells") {
      val ds = (Seq.fill[Tile](10)(injectND(10)(randomTile(10, 10, "uint8"))) :+ null).toDF("tile")
      val expectedNoData = 10 * 10
      val expectedData = 10 * 10 * 10 - expectedNoData

      logger.debug(ds.select($"tile").as[Tile].first.cellType.name)

      assert(ds.select(dataCells($"tile") as "cells").agg(sum("cells")).as[Long].first() === expectedData)
      assert(ds.select(noDataCells($"tile") as "cells").agg(sum("cells")).as[Long].first() === expectedNoData)

      assert(ds.select(aggDataCells($"tile")).first() === expectedData)
      assert(ds.select(aggNoDataCells($"tile")).first() === expectedNoData)

      val resultTileStats = ds.select(tileStats($"tile")("dataCells") as "cells")
        .agg(sum("cells")).as[Long]
        .first()
      assert(resultTileStats === expectedData)
    }

    it("should compute tile statistics") {
      val ds = (Seq.fill[Tile](3)(randomTile(5, 5, "float32")) :+ null).toDS()
      val means1 = ds.select(tileStats($"value")).map(s ⇒ Option(s).map(_.mean).getOrElse(0.0)).collect
      val means2 = ds.select(tileMean($"value")).collect.map(m ⇒ if (m.isNaN) 0.0 else m)
      // Compute the mean manually, knowing we're not dealing with no-data values.
      val means = ds.select(tileToArray[Float]($"value")).map(a ⇒ if (a == null) 0.0 else a.sum/a.length).collect

      forAll(means.zip(means1)) { case (l, r) ⇒ assert(l === r +- 1e-6) }
      forAll(means.zip(means2)) { case (l, r) ⇒ assert(l === r +- 1e-6) }
    }

    it("should compute per-tile histogram") {
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, "float32")).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val r1 = ds.select(tileHistogram($"tiles"))
      assert(r1.first.totalCount === 5 * 5)
      write(r1)

      val r2 = sql("select hist.* from (select rf_tileHistogram(tiles) as hist from tmp)").as[CellHistogram]
      write(r2)
      assert(r1.first.mean === r2.first.mean)
    }

    it("should compute aggregate histogram") {
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, "float32")).toDF("tiles")
      ds.createOrReplaceTempView("tmp")
      val agg = ds.select(aggHistogram($"tiles")).as[CellHistogram]
      val hist = agg.collect()
      assert(hist.length === 1)
      val stats = agg.map(_.stats).as("stats")
      //stats.select("stats.*").show(false)
      assert(stats.first().stddev === 1.0 +- 0.3) // <-- playing with statistical fire :)

      val hist2 = sql("select hist.* from (select rf_aggHistogram(tiles) as hist from tmp)").as[CellHistogram]

      assert(hist2.first.totalCount === 250)
    }

    it("should compute aggregate mean") {
      val ds = (Seq.fill[Tile](10)(randomTile(5, 5, "float32")) :+ null).toDF("tiles")
      val agg = ds.select(aggMean($"tiles"))
      val stats = ds.select(aggStats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(agg.first() === stats.first())
    }

    it("should compute aggregate statistics") {
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, "float32")).toDF("tiles")

      val exploded = ds.select(explodeTiles($"tiles"))
      val (mean, vrnc) = exploded.agg(avg($"tiles"), var_pop($"tiles")).as[(Double, Double)].first

      val stats = ds.select(aggStats($"tiles") as "stats")///.as[(Long, Double, Double, Double, Double)]

      noException shouldBe thrownBy {
        ds.select(aggStats($"tiles")).collect()
      }

      val agg = stats.select($"stats.variance".as[Double])

      assert(vrnc === agg.first() +- 1e-6)

      ds.createOrReplaceTempView("tmp")
      val agg2 = sql("select stats.* from (select rf_aggStats(tiles) as stats from tmp)")
      assert(agg2.first().getAs[Long]("dataCells") === 250L)

      val agg3 = ds.agg(aggStats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(mean === agg3.first())
    }

    it("should compute aggregate local stats") {
      val ave = (nums: Array[Double]) ⇒ nums.sum / nums.length

      val ds = (Seq.fill[Tile](30)(randomTile(5, 5, "float32"))
        .map(injectND(2)) :+ null).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val agg = ds.select(localAggStats($"tiles") as "stats")
      val stats = agg.select("stats.*")

      //printStatsRows(stats)

      val min = agg.select($"stats.min".as[Tile]).map(_.toArrayDouble().min).first
      assert(min < -2.0)
      val max = agg.select($"stats.max".as[Tile]).map(_.toArrayDouble().max).first
      assert(max > 2.0)
      val tendancy = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(tendancy < 0.2)

      val varg = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(varg < 1.1)

      val sqlStats = sql("SELECT stats.* from (SELECT rf_localAggStats(tiles) as stats from tmp)")

      val tiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      val dsTiles = sqlStats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      forEvery(tiles.zip(dsTiles)) { case (t1, t2) ⇒
        assert(t1 === t2)
      }
    }

    it("should compute accurate statistics") {
      val completeTile = squareIncrementingTile(4).convert(IntConstantNoDataCellType)
      val incompleteTile = injectND(2)(completeTile)

      val ds = (Seq.fill(20)(completeTile) :+ null).toDF("tiles")
      val dsNd = (Seq.fill(20)(completeTile) :+ incompleteTile :+ null).toDF("tiles")

      // counted everything properly
      val countTile = ds.select(localAggDataCells($"tiles")).first()
      forAll(countTile.toArray())(i ⇒ assert(i === 20))

      val countArray = dsNd.select(localAggDataCells($"tiles")).first().toArray()
      val expectedCount = (completeTile.localDefined().toArray zip incompleteTile.localDefined().toArray())
          .toSeq.map(pr ⇒ pr._1 * 20 + pr._2)
      assert(countArray === expectedCount)

      val countNodataArray = dsNd.select((localAggNoDataCells($"tiles"))).first().toArray
      assert(countNodataArray === incompleteTile.localUndefined().toArray)

      // GeoTrellis docs do not say how NODATA is treated, but NODATA values are ignored
      val meanTile = dsNd.select(localAggMean($"tiles")).first()
      assert(meanTile.toArray() === completeTile.toArray())

      // GeoTrellis docs state that Min(1.0, NODATA) = NODATA
      val minTile = dsNd.select(localAggMin($"tiles")).first()
      assert(minTile.toArray() === incompleteTile.toArray())

      // GeoTrellis docs state that Max(1.0, NODATA) = NODATA
      val maxTile = dsNd.select(localAggMax($"tiles")).first()
      assert(maxTile.toArray() === incompleteTile.toArray())
    }

    it("should count cells by no-data state") {
      val tsize = 5
      val count = 20
      val nds = 2
      val tiles = (Seq.fill[Tile](count)(randomTile(tsize, tsize, "uint8ud255"))
        .map(injectND(nds)) :+ null).toDF("tiles")

      val counts = tiles.select(noDataCells($"tiles")).collect().dropRight(1)
      forEvery(counts)(c ⇒ assert(c === nds))
      val counts2 = tiles.select(dataCells($"tiles")).collect().dropRight(1)
      forEvery(counts2)(c ⇒ assert(c === tsize * tsize - nds))
    }
  }

  protected def withFixture(test: Any) = ???
}
