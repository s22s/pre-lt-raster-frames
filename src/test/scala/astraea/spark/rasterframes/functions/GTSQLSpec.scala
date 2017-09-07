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

package astraea.spark.rasterframes.functions

import java.nio.file.{Files, Paths}

import astraea.spark.rasterframes._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.local.{Add, Max, Min, Subtract}
import geotrellis.raster.summary.Statistics
import geotrellis.raster.{ByteCellType, CellType, IntConstantNoDataCellType, MultibandTile, Tile, TileFeature}
import geotrellis.spark.{SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.gt._

/**
 * Test rig for Spark UDTs and friends for GT.
 * Extra debugging can be enabled by adding this
 * {{{
 *   import org.apache.spark.sql.execution.debug._
 * }}}
 *
 * @author sfitch
 * @since 3/30/17
 */
class GTSQLSpec extends TestEnvironment with TestData with LazyLogging {
  import GTSQLSpec._
  import TestData.{makeTiles, randomTile}

  /** This is here so we can test writing UDF generated/modified GeoTrellis types to ensure they are Parquet compliant. */
  def write(df: Dataset[_]): Unit = {
    val sanitized = df.select(df.columns.map(c ⇒ col(c).as(c.replaceAll("[ ,;{}()\n\t=]", "_"))): _*)
    val dest = Files.createTempFile(Paths.get(outputLocalPath), "GTSQL", ".parquet")
    logger.debug(s"Writing '${sanitized.columns.mkString(", ")}' to '$dest'...")
    sanitized.write.mode(SaveMode.Overwrite).parquet(dest.toString)
    val rows = df.sparkSession.read.parquet(dest.toString).count()
    logger.debug(s" it has $rows row(s)")
  }

  import sqlContext.implicits._

  sqlContext.udf.register("st_makeTiles", makeTiles)

  describe("Dataframe Ops on GeoTrellis types") {
    it("should resolve column names") {
      // This tests an internal utility.
      assert(col("fred").columnName === "fred")
      assert(col("fred").as("barney").columnName === "barney")
    }

    it("should create constant tiles") {
      val query = sql("select st_makeConstantTile(1, 10, 10, 'int8raw')")
      write(query)
      val tile = query.firstTile
      assert((tile.cellType === ByteCellType) (org.scalactic.Equality.default))
    }

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

    it("should generate multiple rows") {
      val query = sql("select explode(st_makeTiles(3))")
      write(query)
      assert(query.count === 3)
    }

    it("should explode tiles") {
      val query = sql(
        """select st_explodeTiles(
          |  st_makeConstantTile(1, 10, 10, 'int8raw'),
          |  st_makeConstantTile(2, 10, 10, 'int8raw')
          |)
          |""".stripMargin)
      write(query)
      assert(query.select("cell_0", "cell_1").as[(Double, Double)].collect().forall(_ == ((1.0, 2.0))))
      val query2 = sql(
        """|select st_tileDimensions(tiles) as dims, st_explodeTiles(tiles) from (
           |select st_makeConstantTile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      write(query2)
      assert(query2.columns.length === 4)

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTiles($"tile1", $"tile2"))
      //exploded.printSchema()
      assert(exploded.columns.length === 4)
      assert(exploded.count() === 9)
      write(exploded)
    }

    it("should explode tiles with random sampling") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTileSample(0.5, $"tile1", $"tile2"))
      assert(exploded.columns.length === 4)
      assert(exploded.count() < 9)
    }

    it("should code RDD[(Int, Tile)]") {
      val ds = Seq((1, byteArrayTile: Tile)).toDS
      write(ds)
      assert(ds.toDF.as[(Int, Tile)].collect().head === ((1, byteArrayTile)))
    }

    it("should code RDD[Tile]") {
      val rdd = sc.makeRDD(Seq(byteArrayTile: Tile))
      val ds = rdd.toDF("tile")
      write(ds)
      assert(ds.toDF.as[Tile].collect().head === byteArrayTile)
    }

    it("should code RDD[MultibandTile]") {
      val rdd = sc.makeRDD(Seq(multibandTile))
      val ds = rdd.toDS()
      write(ds)
      assert(ds.toDF.as[MultibandTile].collect().head === multibandTile)
    }

    it("should code RDD[TileFeature]") {
      val thing = TileFeature(byteArrayTile: Tile, "meta")
      val ds = Seq(thing).toDS()
      write(ds)
      assert(ds.toDF.as[TileFeature[Tile, String]].collect().head === thing)
    }

    it("should code RDD[Extent]") {
      val ds = Seq(extent).toDS()
      write(ds)
      assert(ds.toDF.as[Extent].collect().head === extent)
    }

    it("should code RDD[ProjectedExtent]") {
      val ds = Seq(pe).toDS()
      write(ds)
      assert(ds.toDF.as[ProjectedExtent].collect().head === pe)
    }

    it("should code RDD[TemporalProjectedExtent]") {
      val ds = Seq(tpe).toDS()
      write(ds)
      assert(ds.toDF.as[TemporalProjectedExtent].collect().head === tpe)
    }

    it("should code RDD[CellType]") {
      val ct = CellType.fromName("uint8")
      val ds = localSeqToDatasetHolder(Seq(ct)).toDS()
      ds.printSchema()
      ds.show(false)
      write(ds)
      assert(ds.toDF.as[CellType].first() === ct)
    }

    it("should code RDD[TileLayerMetadata[SpaceTimeKey]]") {
      val ds = Seq(tlm).toDS()
      ds.printSchema()
      println("end of schema")
      ds.show(false)
      write(ds)
      assert(ds.toDF.as[TileLayerMetadata[SpaceTimeKey]].first() === tlm)
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

    it("should support local algebra") {
      val ds = Seq[(Tile, Tile)]((byteArrayTile, byteConstantTile)).toDF("left", "right")
      ds.createOrReplaceTempView("tmp")

      withClue("add") {
        val sum = ds.select(localAdd($"left", $"right"))
        val expected = Add(byteArrayTile, byteConstantTile)
        assert(sum.as[Tile].first() === expected)

        val sqlSum = sql("select st_localAdd(left, right) from tmp")
        assert(sqlSum.as[Tile].first() === expected)
      }

      withClue("subtract") {
        val sub = ds.select(localSubtract($"left", $"right"))
        val expected = Subtract(byteArrayTile, byteConstantTile)
        assert(sub.as[Tile].first() === expected)

        val sqlSub = sql("select st_localSubtract(left, right) from tmp")
        assert(sqlSub.as[Tile].first() === expected)
      }

    }

    it("should compute tile statistics") {
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, "float32")).toDS()
      val means1 = ds.select(tileStatsDouble($"value")).map(_.mean).collect
      val means2 = ds.select(tileMeanDouble($"value")).collect
      assert(means1 === means2)
    }

    it("should list supported cell types") {
      import gt.types.cellTypes
      val ct = sql("select explode(st_cellTypes())").as[String].collect
      forEvery(cellTypes()) { c ⇒
        assert(ct.contains(c))
      }
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

    it("aggregate functions should handle null tiles") {
      val aggs = Seq(localAggMax _, localAggMin _, localAggCount _)

      val datasets = Seq(
        {
          val tiles = Array.fill[Tile](30)(randomTile(5, 5, "float32"))
          tiles(1) = null
          tiles(11) = null
          tiles(29) = null
          tiles.toSeq
        },
        Seq.fill[Tile](30)(null)
      )

      forEvery(datasets) { tiles ⇒
        val ds = tiles.toDF("tiles")
        val agg = ds.select(localAggStats($"tiles") as "stats")
        val stats = agg.select("stats.*")
        val statTiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
        assert(statTiles.length === 5)

        forEvery(aggs) { aggregate ⇒
          assert(ds.select(aggregate($"tiles")).count() === 1)
        }
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

object GTSQLSpec {
  val rnd = new scala.util.Random(79)

  def injectND(num: Int)(t: Tile): Tile = {
    val indexes = List.tabulate(t.size)(identity)
    val targeted = rnd.shuffle(indexes).take(num)
    def filter(c: Int, r: Int) = targeted.contains(r * t.cols + c)

    if(t.cellType.isFloatingPoint) {
      t.mapDouble((c, r, v) ⇒ {
        if(filter(c,r)) raster.doubleNODATA else v
      })
    }
    else {
      t.map((c, r, v) ⇒ {
        if(filter(c, r)) raster.NODATA else v
      })
    }
  }

  implicit class DFExtras(df: DataFrame) {
    def firstTile: Tile = df.collect().head.getAs[Tile](0)
  }
}
