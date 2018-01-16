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
import astraea.spark.rasterframes._
import _root_.geotrellis.spark.LayerId
import astraea.spark.rasterframes.RasterFrame
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrameReader, Dataset}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col

/**
 * Module utilities.
 *
 * @author sfitch 
 * @since 1/12/18
 */
package object geotrellis {
  case class Layer(base: String, id: LayerId)

  implicit def layerIdEncoder = ExpressionEncoder[Layer]
  /** Convenience column selector for a geotrellis layer. */
  def geotrellis_layer = col("layer").as[Layer]

  /** Set of convenience extension methods on [[org.apache.spark.sql.DataFrameReader]]
   * for querying the geotrellis catalog and loading layers from it. */
  implicit class DataFrameReaderHasGeotrellisFormat(val reader: DataFrameReader) {
    def geotrellisCatalog: DataFrameReader = reader.format("geotrellis-catalog")
    def geotrellis: DataFrameReader = reader.format("geotrellis")
    def geotrellis(id: LayerId): DataFrameReader =
      reader.format("geotrellis")
        .option("layer", id.name)
        .option("zoom", id.zoom.toString)
    def geotrellis(layer: Layer): DataFrameReader =
      reader.format("geotrellis")
        .geotrellis(layer.id)
        .option("path", layer.base)
  }

  /** Extension method on a Dataset[Layer] for loading one or more RasterFrames*/
  implicit class CatalogEntryReader(val selection: Dataset[Layer]) {
    @Experimental
    def readRF: RasterFrame = {
      selection.collect().map { layer ⇒
        val df = selection.sqlContext
          .read
          .geotrellis(layer)
        df.load().asRF
      }.reduce(_ spatialJoin _)
    }
  }
}
