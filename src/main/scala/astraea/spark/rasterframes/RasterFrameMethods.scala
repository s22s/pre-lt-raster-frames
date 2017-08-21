/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark.rasterframes

import geotrellis.raster.resample.{CubicSpline, ResampleMethod}
import geotrellis.raster.{ProjectedRaster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling.{LayoutDefinition, Tiler}
import geotrellis.util.MethodExtensions
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.Metadata
import spray.json._

/**
 * Extension methods on [[RasterFrame]] type.
 * @author sfitch
 * @since 7/18/17
 */
trait RasterFrameMethods extends MethodExtensions[RasterFrame] {
  /** Get the names of the columns that are of type `Tile` */
  def tileColumns: Seq[String] = self.schema.fields
    .filter(_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
    .map(_.name)

  /** Get the spatial column. */
  def spatialKeyColumn: String = {
    val key = self.schema.fields.find(_.metadata.contains(CONTEXT_METADATA_KEY))
    require(key.nonEmpty, "All RasterFrames must have a column tagged with context")
    key.get.name
  }

  def tileLayerMetadata[K: SpatialComponent: JsonFormat]: TileLayerMetadata[K] = {
    self.schema
      .find(_.name == SPATIAL_KEY_COLUMN)
      .map(_.metadata)
      .map(extract[TileLayerMetadata[K]](CONTEXT_METADATA_KEY))
      .get
  }

  private def extract[M: JsonFormat](metadataKey: String)(md: Metadata) = {
    md.getMetadata(metadataKey).json.parseJson.convertTo[M]
  }

  def toRaster(tileCol: Column, rasterCols: Int, rasterRows: Int,
    resampler: ResampleMethod = CubicSpline): ProjectedRaster[Tile] = {

    val df = self
    import df.sqlContext.implicits._

    // TODO: support STK too.
    val md = tileLayerMetadata[SpatialKey]
    val keyCol = spatialKeyColumn
    val newLayout = LayoutDefinition(md.extent, TileLayout(1, 1, rasterCols, rasterRows))
    val newLayerMetadata = md.copy(layout = newLayout, bounds = Bounds(SpatialKey(0, 0), SpatialKey(1, 1)))

    val rdd: RDD[(SpatialKey, Tile)] = self.select(col(keyCol), tileCol).as[(SpatialKey, Tile)].rdd
    val newLayer = rdd
      .map { case (key, tile) ⇒
        (ProjectedExtent(md.mapTransform(key), md.crs), tile)
      }
      .tileToLayout(newLayerMetadata, Tiler.Options(resampler))

    // Attempt to get cross-tile interpolation to work. Generates empty tiles.
//    val bufferSize = 2
//    val newLayer = rdd.bufferTiles(bufferSize)
//      .map { case (key, BufferedTile(tile, targetBounds)) ⇒
//        val keyExtent = md.mapTransform(key)
//        val re = RasterExtent(keyExtent, targetBounds.width, targetBounds.height)
//        val adjustedExtent = re.rasterExtentFor(GridBounds(-bufferSize, -bufferSize, re.cols - 1 + bufferSize, re.rows - 1 + bufferSize))
//        (ProjectedExtent(adjustedExtent.extent, md.crs), tile)
//      }
//      .tileToLayout(newLayerMetadata, Tiler.Options(resampler))

    val stitchedTile = newLayer.stitch()

    val croppedTile = stitchedTile.crop(rasterCols, rasterRows)

    ProjectedRaster(croppedTile, md.extent, md.crs)
  }
}

