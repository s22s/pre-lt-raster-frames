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

package org.apache.spark.sql.gt

import java.sql.Timestamp

import geotrellis.raster.{ByteCellType, MultibandTile, Tile, TileFeature}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.gt.functions._
import org.apache.spark.sql.{DataFrame, Encoders}
import org.scalatest.{FunSpec, Inspectors, Matchers}
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.execution.debug._

/**
 * Test rig for Spark UDTs and friends for GT.
 * @author sfitch
 * @since 3/30/17
 */
class GTSQLSpec extends FunSpec with Matchers with Inspectors with TestEnvironment with TestData {

  gtRegister(sql)

  implicit class DFExtras(df: DataFrame) {
    def firstTile: Tile = df.collect().head.getAs[Tile](0)
  }

  import _spark.implicits._

  describe("GeoTrellis UDTs") {
    it("should create constant tiles") {
      val query = sql.sql("select st_makeConstantTile(1, 10, 10, 'int8raw')")
      val tile = query.firstTile
      assert((tile.cellType === ByteCellType) (org.scalactic.Equality.default))
    }

    it("should evaluate UDF on tile") {
      val query = sql.sql("select st_focalSum(st_makeConstantTile(1, 10, 10, 'int8raw'), 4)")
      val tile = query.firstTile
      println(tile.asciiDraw())
      assert(tile.cellType === ByteCellType)
    }

    it("should report dimensions") {
      val query = sql.sql(
        """|select st_gridRows(tiles) as rows, st_gridCols(tiles) as cols from (
           |select st_makeConstantTile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      assert(query.as[(Int, Int)].collect().head === ((10, 10)))
    }

    it("should generate multiple rows") {
      val query = sql.sql("select st_makeTiles(3)")
      val tiles = query.collect().head.getAs[Seq[Tile]](0)
      assert(tiles.distinct.size == 1)
    }

    it("should explode rows") {
      val query = sql.sql(
        """select st_explodeTile(
          |  st_makeConstantTile(1, 10, 10, 'int8raw'),
          |  st_makeConstantTile(2, 10, 10, 'int8raw')
          |)
          |""".stripMargin)
      assert(query.select("cell_0", "cell_1").as[(Double, Double)].collect().forall(_ == ((1.0, 2.0))))
      val query2 = sql.sql(
        """|select st_gridRows(tiles) as rows, st_gridCols(tiles) as cols, st_explodeTile(tiles)  from (
           |select st_makeConstantTile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      assert(query2.columns.size === 5)

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTile($"tile1", $"tile2"))

      exploded.printSchema()
      exploded.show()

    }

    it("should code RDD[(Int, Tile)]") {
      val ds = Seq((1, byteArrayTile: Tile)).toDS
      assert(ds.toDF.as[(Int, Tile)].collect().head === ((1, byteArrayTile)))
    }

    it("should code RDD[Tile]") {
      val rdd = sc.makeRDD(Seq(byteArrayTile: Tile))
      val ds = rdd.toDF("tile")
      assert(ds.toDF.as[Tile].collect().head === byteArrayTile)
    }

    it("should code RDD[MultibandTile]") {
      val rdd = sc.makeRDD(Seq(multibandTile))
      val ds = rdd.toDS()
      assert(ds.toDF.as[MultibandTile].collect().head === multibandTile)
    }

    it("should code RDD[TileFeature]") {
      val thing = TileFeature(byteArrayTile: Tile, "meta")
      val ds = Seq(thing).toDS()
      assert(ds.toDF.as[TileFeature[Tile, String]].collect().head === thing)
    }

    it("should code RDD[Extent]") {
      val ds = Seq(extent).toDS()
      assert(ds.toDF.as[Extent].collect().head === extent)
    }

    it("should code RDD[ProjectedExtent]") {
      val ds = Seq(pe).toDS()
      assert(ds.toDF.as[ProjectedExtent].collect().head === pe)
    }

    it("should code RDD[TemporalProjectedExtent]") {
      val ds = Seq(tpe).toDS()
      assert(ds.toDF.as[TemporalProjectedExtent].collect().head === tpe)
    }

    it("should flatten stuff") {
      withClue("Extent") {
        val ds = Seq(extent).toDS()
        assert(ds.select(flatten(asStruct($"value".as[Extent]))).select("xmax").as[Double].collect().head === extent.xmax)
        assert(ds.select(flatten($"value".as[Extent])).select("xmax").as[Double].collect().head === extent.xmax)
        assert(ds.select(expr("st_flattenExtent(value)")).select("xmax").as[Double].collect().head === extent.xmax)
      }
      withClue("ProjectedExtent") {
        val ds = Seq(pe).toDS()
        val flattened = ds
          .select(flatten($"value".as[ProjectedExtent]))
          .select(flatten($"extent".as[Extent]), $"crs")
        assert(flattened.select("xmax").as[Double].collect().head === pe.extent.xmax)
      }
      withClue("ProjectedExtent") {
        val ds = Seq(tpe).toDS()
        val flattened = ds
          .select(flatten($"value".as[TemporalProjectedExtent]))
          .select(flatten($"extent".as[Extent]), $"time", $"crs")
        implicit val enc = Encoders.TIMESTAMP

        assert(flattened.select("time").as[Timestamp].collect().head === new Timestamp(tpe.instant))
      }
    }
  }
}
