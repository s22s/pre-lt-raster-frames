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

package astraea.spark.rasterframes.encoders

import geotrellis.proj4.CRS
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.summary.Statistics
import geotrellis.raster.{CellType, MultibandTile, Tile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Implicit encoder for RasterFrame types.
 */
trait EncoderImplicits {
  implicit val singlebandTileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  implicit val multibandTileEncoder: Encoder[MultibandTile] = ExpressionEncoder()
  implicit val crsEncoder: ExpressionEncoder[CRS] = CRSEncoder()
  implicit val extentEncoder: ExpressionEncoder[Extent] = ExpressionEncoder()
  implicit val projectedExtentEncoder = ProjectedExtentEncoder()
  implicit val temporalProjectedExtentEncoder = TemporalProjectedExtentEncoder()
  implicit val histogramDoubleEncoder: Encoder[Histogram[Double]] = ExpressionEncoder()
  implicit val histogramIntEncoder: Encoder[Histogram[Int]] = ExpressionEncoder()
  implicit val statsEncoder: Encoder[Statistics[Double]] = ExpressionEncoder()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: Encoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder[K]()
  implicit val layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = ExpressionEncoder()
  implicit val stkBoundsEncoder: ExpressionEncoder[KeyBounds[SpaceTimeKey]] = ExpressionEncoder()
  implicit val cellTypeEncoder: ExpressionEncoder[CellType] = CellTypeEncoder()
  implicit val spatialKeyEncoder: ExpressionEncoder[SpatialKey] = ExpressionEncoder()
  implicit val temporalKeyEncoder: ExpressionEncoder[TemporalKey] = ExpressionEncoder()
  implicit val spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = ExpressionEncoder()
}
object EncoderImplicits extends EncoderImplicits
