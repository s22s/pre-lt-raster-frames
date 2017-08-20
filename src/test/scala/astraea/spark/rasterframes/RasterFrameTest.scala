

package astraea.spark.rasterframes

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.LatLng
import geotrellis.raster.render.{ColorMap, ColorMaps, ColorRamp, ColorRamps}
import geotrellis.raster.{ProjectedRaster, Tile, TileFeature, TileLayout}
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
  import TestData.randomTile

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

      val tileRDD = inputRdd.map {case (k, v) ⇒ (metadata.mapTransform(k.extent.center), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = WithTFContextRDDMethods(tileLayerRDD).toRF

      rf.show(false)
    }

    it("should convert a GeoTiff to RasterFrame") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val (cols, rows) = praster.raster.dimensions

      val layoutCols = math.ceil(cols / 128.0).toInt
      val layoutRows = math.ceil(rows / 128.0).toInt

      assert(praster.toRF.count() === 1)
      assert(praster.toRF(128, 128).count() === (layoutCols * layoutRows))
    }

    it("should provide TileLayerMetadata") {
      val rf = sampleGeoTiff.projectedRaster.toRF(256, 256)
      val tlm = rf.tileLayerMetadata[SpatialKey]
      assert(tlm.bounds.get._1 === SpatialKey(0, 0))
      assert(tlm.bounds.get._2 === SpatialKey(4, 2))
    }

    def Greyscale(stops: Int): ColorRamp = {
      val colors = (0 to stops)
        .map(i ⇒ {
          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
          (c << 8) | 0xFF // Add alpha channel.
        })
      ColorRamp(colors)
    }

    it("should restitch to raster") {
      // 774 × 500
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      println("1", praster.projectedExtent)
      val (cols, rows) = praster.raster.dimensions
      val rf = praster.toRF(64, 64)
      println("2", rf.tileLayerMetadata[SpatialKey].extent)
      val raster = rf.toRaster($"tile", cols, rows)
      println(raster.extent)

      val colors = ColorMap.fromQuantileBreaks(raster.tile.histogram, Greyscale(128))
      raster.tile.color(colors).renderPng().write("/tmp/test.png")


      assert(raster.raster.dimensions ===  (cols, rows))

    }
  }
}
