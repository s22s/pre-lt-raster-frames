/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.util

import java.net.URI

import astraea.spark.rasterframes._
import geotrellis.proj4.WebMercator
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpatialKey}
import geotrellis.spark.io.slippy.HadoopSlippyTileWriter
import geotrellis.spark.tiling.ZoomedLayoutScheme
import org.apache.spark.annotation.Experimental

/**
 * Additional debugging routines. No guarantees these are or will remain stable.
 *
 * @since 4/6/18
 */
package object debug {
  implicit class RasterFrameWithDebug(val self: RasterFrame)  {

    /** Renders the whole schema with metadata as a JSON string. */
    def describeFullSchema: String = {
      self.schema.prettyJson
    }
  }
}
