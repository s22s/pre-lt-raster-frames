/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

import astraea.spark.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.SQLGeometricConstructorFunctions.ST_MakePoint
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfter

/**
 * @author echeipesh
 */
class GeoTrellisCatalogSpec
    extends TestEnvironment with TestData with BeforeAndAfter
    with IntelliJPresentationCompilerHack {

  val now = ZonedDateTime.now()
  val tileCoordRange = 2 to 5

//  lazy val testRdd = {
//    val recs: Seq[(SpaceTimeKey, Tile)] = for {
//      col ← tileCoordRange
//      row ← tileCoordRange
//    } yield SpaceTimeKey(col, row, now) -> ArrayTile.alloc(DoubleConstantNoDataCellType, 3, 3)
//
//    val rdd = sc.parallelize(recs)
//    val scheme = ZoomedLayoutScheme(LatLng, tileSize = 3)
//    val layerLayout = scheme.levelForZoom(4).layout
//    val layerBounds = KeyBounds(SpaceTimeKey(2, 2, now.minusMonths(1)), SpaceTimeKey(5, 5, now.plusMonths(1)))
//    val md = TileLayerMetadata[SpaceTimeKey](
//      cellType = DoubleConstantNoDataCellType,
//      crs = LatLng,
//      bounds = layerBounds,
//      layout = layerLayout,
//      extent = layerLayout.mapTransform(layerBounds.toGridBounds()))
//    ContextRDD(rdd, md)
//  }

  lazy val testRdd = TestData.randomSpatioTemporalTileLayerRDD(10, 12, 5, 6)


  before {
    val outputDir = new File(outputLocalPath)
    FileUtil.fullyDelete(outputDir)
    outputDir.deleteOnExit()
    lazy val writer = LayerWriter(outputDir.toURI)
    val index =  ZCurveKeyIndexMethod.byDay()
    writer.write(LayerId("layer-1", 0), testRdd, index)
    writer.write(LayerId("layer-2", 0), testRdd, index)
  }

  describe("Catalog reading") {
    val catalogReader = sqlContext.read
      .format("geotrellis-catalog")
      .option("path", outputLocal.toUri.toString)

    it("should show two zoom levels") {
      val cat = catalogReader.load()
      cat.show()
      assert(cat.schema.length > 4)
      assert(cat.count() === 2)
    }
  }
}
