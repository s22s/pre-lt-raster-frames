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

import astraea.spark.rasterframes._
import geotrellis.raster.{CellType, MultibandTile, Tile, TileFeature}
import geotrellis.spark.{SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}

/**
 * Test rig for encoding GT types into Catalyst types.
 *
 * @author sfitch 
 * @since 9/18/17
 */
class EncodingSpec extends TestEnvironment with TestData  {

  import sqlContext.implicits._

  describe("Dataframe encoding ops on GeoTrellis types") {

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
  }
}