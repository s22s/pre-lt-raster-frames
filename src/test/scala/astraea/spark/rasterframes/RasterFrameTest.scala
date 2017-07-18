/*
 * Copyright (c) 2017. Astraea, Inc. All rights reserved.
 */

package astraea.spark.rasterframes

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.LatLng
import geotrellis.spark.io._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import org.apache.spark.sql.gt.functions._

/**
 * RasterFrame test rig.
 *
 * @author sfitch 
 * @since 7/10/17
 */
class RasterFrameTest extends TestEnvironment with TestData with LazyLogging {

  describe("RasterFrame") {
    it("should implicitly convert from layer type") {
      implicit val spark = sqlContext.sparkSession
      import spark.implicits._

      val tile = randomTile(20, 20, "uint8")

      val tileLayerRDD = TileLayerRDDBuilders.createTileLayerRDD(tile, 2, 2, LatLng)._2

      val rf = tileLayerRDD.toRF

      rf.printSchema()
      rf.orderBy("key").show(false)
      println(rf.schema.head.metadata.json)

      assert(rf.schema.head.metadata.contains("extent"))

      assert(
        rf.select(tileDimensions($"tile"))
          .as[Tuple1[(Int, Int)]]
          .map(_._1)
          .collect()
          .forall(_ == (10, 10))
      )

      assert(rf.count() === 4)
    }
  }
}
