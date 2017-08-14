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

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.util.MethodExtensions
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


//  def toGeoTIFF(): GeoTiff = {
//
//  }

//  def spatialJoin(right: RasterFrame): RasterFrame = {
//    val metadata = self.schema.head.metadata
//    // TODO: WARNING this is not a proper spatial join, but just a placeholder.
//    logger.warn("Multi-layer query assumes same tile layout")
//    val joined = self.join(right, Seq("key"), "outer")
//    joined.setColumnMetadata("key", metadata)
//  }
}

