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

package org.apache.spark.sql.gt

import geotrellis.proj4.CRS
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.summary.Statistics
import geotrellis.raster.{CellType, MultibandTile, Tile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types._

import scala.reflect.runtime.universe._

trait Implicits {
  implicit def singlebandTileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  implicit def multibandTileEncoder: Encoder[MultibandTile] = ExpressionEncoder()
  implicit def crsEncoder: ExpressionEncoder[CRS] = CRSEncoder()
  implicit def extentEncoder: ExpressionEncoder[Extent] = ExpressionEncoder()
  implicit def projectedExtentEncoder = ProjectedExtentEncoder()
  implicit def temporalProjectedExtentEncoder = TemporalProjectedExtentEncoder()
  implicit def histogramDoubleEncoder: Encoder[Histogram[Double]] = ExpressionEncoder()
  implicit def histogramIntEncoder: Encoder[Histogram[Int]] = ExpressionEncoder()
  implicit def histogramStatsEncoder: Encoder[Statistics[Double]] = ExpressionEncoder()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: Encoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder[K]()
  implicit def layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = ExpressionEncoder()
  implicit def stkBoundsEncoder: ExpressionEncoder[KeyBounds[SpaceTimeKey]] = ExpressionEncoder()
  implicit def cellTypeEncoder: ExpressionEncoder[CellType] = CellTypeEncoder()
  implicit def spatialKeyEncoder: ExpressionEncoder[SpatialKey] = ExpressionEncoder()
  implicit def temporalKeyEncoder: ExpressionEncoder[TemporalKey] = ExpressionEncoder()
  implicit def spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = ExpressionEncoder()
}
object Implicits extends Implicits
