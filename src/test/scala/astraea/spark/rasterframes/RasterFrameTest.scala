

package astraea.spark.rasterframes

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.LatLng
import geotrellis.raster.{Tile, TileFeature, TileLayout}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.tiling._
import geotrellis.vector.ProjectedExtent

/**
 * RasterFrame test rig.
 *
 * @author sfitch 
 * @since 7/10/17
 */
class RasterFrameTest extends TestEnvironment with TestData with LazyLogging {
  import spark.implicits._

  describe("RasterFrame") {
    it("should implicitly convert from layer type") {

      val tile = randomTile(20, 20, "uint8")

      val tileLayerRDD: TileLayerRDD[SpatialKey] =
        TileLayerRDDBuilders.createTileLayerRDD(tile, 2, 2, LatLng)._2

      val rf = tileLayerRDD.toRF

      assert(rf.tileColumns.nonEmpty)
      assert(rf.spatialKeyColumn == "key")

      rf.printSchema()
      rf.orderBy("key").show(false)

      assert(rf.schema.head.metadata.contains(CONTEXT_METADATA_KEY))
      assert(rf.schema.head.metadata.json.contains("tileLayout"))

      assert(
        rf.select(tileDimensions($"tile"))
          .as[Tuple1[(Int, Int)]]
          .map(_._1)
          .collect()
          .forall(_ == (10, 10))
      )

      assert(rf.count() === 4)
    }

    it("should implicitly convert layer of TileFeature") {


      val tile = TileFeature(randomTile(20, 20, "uint8"), (1, "b", 3.0))

      val tileLayout = TileLayout(1, 1, 20, 20)

      val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
      val inputRdd = sc.parallelize(Seq((ProjectedExtent(LatLng.worldExtent, LatLng), tile)))

      val (_, metadata) = inputRdd.collectMetadata[SpatialKey](LatLng, layoutScheme)

      val tileRDD = inputRdd.map{case (k, v) ⇒ (metadata.mapTransform(k.extent.center), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = WithTFContextRDDMethods(tileLayerRDD).toRF

      rf.show(false)

    }
  }
}
