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

import astraea.spark.rasterframes._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import org.apache.hadoop.fs.FileUtil
import org.scalatest.BeforeAndAfter

/**
 * @author echeipesh
 */
class GeoTrellisCatalogSpec
    extends TestEnvironment with TestData with BeforeAndAfter
    with IntelliJPresentationCompilerHack {

  lazy val testRdd = TestData.randomSpatioTemporalTileLayerRDD(10, 12, 5, 6)

  import sqlContext.implicits._

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
    it("should show two zoom levels") {
      val cat = sqlContext.read
        .format("geotrellis-catalog")
        .load(outputLocal.toUri.toString)
      cat.show()
      assert(cat.schema.length > 4)
      assert(cat.count() === 2)
    }

    it("should support loading a layer in a nice way") {
      val cat = sqlContext.read
        .format("geotrellis-catalog")
        .load(outputLocal.toUri.toString)

      cat.limit(2)
        .select(catalog_layer).readRF
        .show(false)

    }
  }
}
