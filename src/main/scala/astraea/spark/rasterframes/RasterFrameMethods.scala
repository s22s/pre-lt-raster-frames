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

import geotrellis.raster.resample.{Bilinear, ResampleMethod}
import geotrellis.raster.{ProjectedRaster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling.{LayoutDefinition, Tiler}
import geotrellis.util.MethodExtensions
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, TypedColumn}
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
  type TileColumn = TypedColumn[Any, Tile]

  private val _df = self
  import _df.sqlContext.implicits._

  /** Get the names of the columns that are of type `Tile` */
  def tileColumns: Seq[TileColumn] =
    self.schema.fields
      .filter(_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
      .map(f ⇒ self(f.name).as[Tile])

  /** Get the spatial column. */
  def spatialKeyColumn: TypedColumn[Any, SpatialKey] = {
    val spark = self.sparkSession
    import spark.implicits._
    val key = findSpatialKeyField
    require(key.nonEmpty, "All RasterFrames must have a column tagged with context")
    self(key.get.name).as[SpatialKey]
  }

  /** The spatial key is the first on found with context metadata attached to it. */
  private[rasterframes] def findSpatialKeyField =
    self.schema.fields.find(_.metadata.contains(CONTEXT_METADATA_KEY))

  /**
   * Reassemble the [[TileLayerMetadata]] record from DataFrame metadata.
   * TODO: Change to Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]]
   */
  def tileLayerMetadata: TileLayerMetadata[SpatialKey] =
    self.schema
      .find(_.name == SPATIAL_KEY_COLUMN)
      .map(_.metadata)
      .map(extract[TileLayerMetadata[SpatialKey]](CONTEXT_METADATA_KEY))
      .getOrElse(throw new IllegalArgumentException(s"RasterFrame operation requsted on non-RasterFrame: $self"))

  /**
   * Performs a full RDD scans of the key column for the data extent, and updates the [[TileLayerMetadata]] data extent to match.
   */
  def clipLayerExtent: RasterFrame = {
    val metadata = tileLayerMetadata

    val trans = metadata.layout.mapTransform
    val keyBounds = self
      .select(spatialKeyColumn)
      .map(k ⇒ KeyBounds(k, k))
      .reduce(_ combine _)

    val gridExtent = trans(keyBounds.toGridBounds())

    val newExtent = gridExtent.intersection(metadata.extent).getOrElse(gridExtent)

    val updatedMetadata = metadata.copy(extent = newExtent, bounds = keyBounds)

    self.addColumnMetadata(spatialKeyColumn, CONTEXT_METADATA_KEY, updatedMetadata.asColumnMetadata).certify
  }

  /**
   * Convert from RasterFrame to a GeoTrellis [[TileLayerMetadata]]
   * @param tileCol column with tiles to be the
   */
  def toTileLayerRDD(tileCol: TileColumn): TileLayerRDD[SpatialKey] =
    ContextRDD(self.select(spatialKeyColumn, tileCol).rdd, tileLayerMetadata)

  /** Extract metadata value. */
  private def extract[M: JsonFormat](metadataKey: String)(md: Metadata) =
    md.getMetadata(metadataKey).json.parseJson.convertTo[M]

  /** Convert the tiles in the RasterFrame into a single raster. */
  def toRaster(tileCol: Column,
               rasterCols: Int,
               rasterRows: Int,
               resampler: ResampleMethod = Bilinear): ProjectedRaster[Tile] = {

    val clipped = clipLayerExtent

    val md = clipped.tileLayerMetadata
    val keyCol = clipped.spatialKeyColumn
    val newLayout = LayoutDefinition(md.extent, TileLayout(1, 1, rasterCols, rasterRows))
    val newLayerMetadata = md.copy(layout = newLayout, bounds = Bounds(SpatialKey(0, 0), SpatialKey(0, 0)))

    val rdd: RDD[(SpatialKey, Tile)] = clipped.select(keyCol, tileCol).as[(SpatialKey, Tile)].rdd
    val newLayer = rdd
      .map {
        case (key, tile) ⇒
          (ProjectedExtent(md.mapTransform(key), md.crs), tile)
      }
      .tileToLayout(newLayerMetadata, Tiler.Options(resampler))

    val stitchedTile = newLayer.stitch()

    val croppedTile = stitchedTile.crop(rasterCols, rasterRows)

    ProjectedRaster(croppedTile, md.extent, md.crs)
  }
}
