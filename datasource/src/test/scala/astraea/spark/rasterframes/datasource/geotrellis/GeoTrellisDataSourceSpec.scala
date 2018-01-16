/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2018 Azavea & Astraea, Inc.
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
package astraea.spark.rasterframes.datasource.geotrellis

import java.io.File
import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.locationtech.geomesa.spark.SQLGeometricConstructorFunctions._
import org.scalatest.BeforeAndAfter
import astraea.spark.rasterframes._

/**
 * @author echeipesh
 */
class GeoTrellisDataSourceSpec
    extends TestEnvironment with TestData with BeforeAndAfter
    with IntelliJPresentationCompilerHack {

  val now = ZonedDateTime.now()
  val tileCoordRange = 2 to 5

  lazy val testRdd = {
    val recs: Seq[(SpaceTimeKey, Tile)] = for {
      col ← tileCoordRange
      row ← tileCoordRange
    } yield SpaceTimeKey(col, row, now) -> ArrayTile.alloc(DoubleConstantNoDataCellType, 3, 3)

    val rdd = sc.parallelize(recs)
    val scheme = ZoomedLayoutScheme(LatLng, tileSize = 3)
    val layerLayout = scheme.levelForZoom(4).layout
    val layerBounds = KeyBounds(SpaceTimeKey(2, 2, now.minusMonths(1)), SpaceTimeKey(5, 5, now.plusMonths(1)))
    val md = TileLayerMetadata[SpaceTimeKey](
      cellType = DoubleConstantNoDataCellType,
      crs = LatLng,
      bounds = layerBounds,
      layout = layerLayout,
      extent = layerLayout.mapTransform(layerBounds.toGridBounds()))
    ContextRDD(rdd, md)
  }

  before {
    val outputDir = new File(outputLocalPath)
    FileUtil.fullyDelete(outputDir)
    outputDir.deleteOnExit()
    lazy val writer = LayerWriter(outputDir.toURI)
    // TestEnvironment will clean this up
    writer.write(LayerId("all-ones", 4), testRdd, ZCurveKeyIndexMethod.byDay())
  }

  describe("DataSource reading") {
    val layerReader = sqlContext.read.geotrellis
      .option("path", outputLocal.toUri.toString)
      .option("layer", "all-ones")
      .option("zoom", "4")

    it("should read tiles") {
      val df = layerReader.load()
      assert(df.count === tileCoordRange.length * tileCoordRange.length)
    }

    it("used produce tile UDT that we can manipulate") {
      val df = layerReader.load().select(SPATIAL_KEY_COLUMN, tileStats(TILE_COLUMN))
      assert(df.count() > 0)
    }

    it("should respect bbox query") {
      val boundKeys = KeyBounds(SpatialKey(3, 4), SpatialKey(4, 4))
      val bbox = testRdd.metadata.layout
        .mapTransform(boundKeys.toGridBounds())
        .jtsGeom
      val df = layerReader.load().asRF.withCenter().where(CENTER_COLUMN intersects bbox)
      assert(df.count() === boundKeys.toGridBounds.sizeLong)
    }

    it("should invoke Encoder[Extent]") {
      val df = layerReader.load().asRF.withExtent()
      assert(df.count > 0)
      assert(df.first.length === 5)
      assert(df.first.getAs[Extent](2) !== null)
    }
  }
  describe("Predicate push-down support") {

    def extractRelation(df: DataFrame) = {
      val plan = df.queryExecution.optimizedPlan
      plan.children.collect {
        case LogicalRelation(gt: GeoTrellisRelation, _, _) ⇒ gt
      }.head
    }

    val layerReader = sqlContext.read
      .format("geotrellis")
      .option("path", outputLocal.toUri.toString)
      .option("layer", "all-ones")
      .option("zoom", "4")

    val pt1 = ST_MakePoint(-88, 60)
    val pt2 = ST_MakePoint(-78, 38)

    val targetKey = testRdd.metadata.mapTransform(Point(pt1))

    it("should support extent against a geometry literal") {
      val df = layerReader
        .load()
        .where(EXTENT_COLUMN intersects pt1)

      val rel = extractRelation(df)
      assert(rel.filters.length === 1)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    it("should *not* support extent filter against a UDF") {
      val targetKey = testRdd.metadata.mapTransform(Point(pt1))

      val mkPtFcn = udf((_: Row) ⇒ { ST_MakePoint(-88, 60) })

      val df = layerReader
        .load()
        .where(intersects(EXTENT_COLUMN, mkPtFcn(SPATIAL_KEY_COLUMN)))

      assert(extractRelation(df).filters.length === 0)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    it("should support temporal predicats") {
      withClue("at now") {
        val df = layerReader
          .load()
          .where(TIMESTAMP_COLUMN at now)

        assert(extractRelation(df).filters.length == 1)
        assert(df.count() == testRdd.count())
      }

      withClue("at earlier") {
        val df = layerReader
          .load()
          .where(TIMESTAMP_COLUMN at now.minusDays(1))

        assert(extractRelation(df).filters.length == 1)
        assert(df.count() == 0)
      }

      withClue("between now") {
        val df = layerReader
          .load()
          .where(TIMESTAMP_COLUMN betweenTimes (now.minusDays(1), now.plusDays(1)))

        assert(extractRelation(df).filters.length == 1)
        assert(df.count() == testRdd.count())
      }

      withClue("between later") {
        val df = layerReader
          .load()
          .where(TIMESTAMP_COLUMN betweenTimes (now.plusDays(1), now.plusDays(2)))

        assert(extractRelation(df).filters.length == 1)
        assert(df.count() == 0)
      }
    }

    it("should support nested predicates") {
      val df = layerReader
        .load()
        .where(
          ((EXTENT_COLUMN intersects pt1) ||
            (EXTENT_COLUMN intersects pt2)) &&
            (TIMESTAMP_COLUMN at now)
        )

      val rel = extractRelation(df)
      assert(rel.filters.length === 1)
      assert(rel.splitFilters.length === 2, rel.splitFilters.toString)

      assert(df.count === 2)
    }
  }
}
