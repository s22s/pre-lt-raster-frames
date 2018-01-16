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

package astraea.spark.rasterframes.datasource
import java.net.URI

import _root_.geotrellis.spark.LayerId
import astraea.spark.rasterframes
import astraea.spark.rasterframes.util.toParquetFriendlyColumnName
import astraea.spark.rasterframes.{RasterFrame, _}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset}
import shapeless.tag.@@
import shapeless.tag

/**
 * Module utilities.
 *
 * @author sfitch 
 * @since 1/12/18
 */
package object geotrellis {
  implicit val layerEncoder = Layer.layerEncoder

  /** Convenience column selector for a GeoTrellis layer. */
  def geotrellis_layer = col("layer").as[Layer]

  /** Tagged type construction for enabling type-safe extension methods for loading
   * a RasterFrame from a GeoTrellis layer. */
  type GeoTrellisRasterFrameReader = DataFrameReader @@ GeoTrellisRasterFrameReaderTag
  trait GeoTrellisRasterFrameReaderTag

//  /** Tagged type construction for enabling type-safe extension methods for loading
//   * a DataFrame describing DataFrame layers. */
//  type GeoTrellisCatalogReader = DataFrameReader @@ GeoTrellisCatalogDataFrameReaderTag
//  trait GeoTrellisCatalogDataFrameReaderTag

  /** Set of convenience extension methods on [[org.apache.spark.sql.DataFrameReader]]
   * for querying the GeoTrellis catalog and loading layers from it. */
  implicit class DataFrameReaderHasGeotrellisFormat(val reader: DataFrameReader) {
    /** Read the GeoTrellis Catalog of layers from a base path. */
    def geotrellisCatalog(base: URI): DataFrame =
      reader.format("geotrellis-catalog").load(base.toASCIIString)

    def geotrellis: GeoTrellisRasterFrameReader =
      tag[GeoTrellisRasterFrameReaderTag][DataFrameReader](reader.format("geotrellis"))
  }

  /** Extension methods for loading a RasterFrame from a tagged `DataFrameReader`. */
  implicit class GeoTrellisReaderWithRF(val reader: GeoTrellisRasterFrameReader) {
    def loadRF(uriPath: String, id: LayerId): RasterFrame =
      reader
        .option("layer", id.name)
        .option("zoom", id.zoom.toString)
        .load(uriPath)
        .asRF

    def loadRF(layer: Layer): RasterFrame = loadRF(layer.base.toASCIIString, layer.id)
  }

  /** Extension method on a Dataset[Layer] for loading one or more RasterFrames*/
  implicit class CatalogEntryReader(val selection: Dataset[Layer]) {
    def loadRF: RasterFrame = {
      val TC = TILE_COLUMN.columnName
      val layers = selection.collect()

      val rfs = layers.map { layer ⇒
        selection.sparkSession
          .read
          .geotrellis
          .loadRF(layer)
      }

      val renamed = if(layers.length > 1) {
        rfs.zip(layers).map { case (rf, layer) ⇒
          val newName = toParquetFriendlyColumnName(s"${TC}_${layer.id.name}")
          rf
            .withColumnRenamed(TC, newName)
            .certify
        }
      }
      else rfs

      renamed
        .reduceOption(_ spatialJoin _)
        .getOrElse(throw new IllegalArgumentException("Cannot load empty selection."))
    }
  }
}
