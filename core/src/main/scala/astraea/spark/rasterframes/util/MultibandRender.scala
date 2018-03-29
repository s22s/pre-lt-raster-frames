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

import geotrellis.raster._
import geotrellis.raster.render.Png

/**
 * Rework of process courtesy of @lossyrob for creating natural color RGB images in GeoTrellis.
 *
 * Source: https://goo.gl/9ewJCG
 *
 * @since 3/27/18
 */
object MultibandRender {
  object CellTransforms {
    def clamp(min: Int, max: Int)(z: Int) = {
      if(isData(z)) { if(z > max) { max } else if(z < min) { min } else { z } }
      else { z }
    }

    val clampByte = clamp(0, 255) _

    def brightnessCorrect(brightness: Int)(v: Int): Int =
      if(v > 0) { v + brightness }
      else { v }

    def contrastCorrect(contrast: Int)(v: Int): Int = {
      val contrastFactor = (259 * (contrast + 255)) / (255 * (259 - contrast))
      (contrastFactor * (v - 128)) + 128
    }

    def gammaCorrect(gamma: Double)(v: Int): Int = {
      val gammaCorrection = 1 / gamma
      (255 * math.pow(v / 255.0, gammaCorrection)).toInt
    }
  }
  import CellTransforms._

  trait Profile {
    /** Value from -255 to 255 */
    val brightness: Int = 0
    /** Value from  -255 to 255 */
    val contrast: Int = 0
    /**  0.01 to 7.99 */
    val gamma: Double = 1.0

    /** Get the red band. */
    def red(mb: MultibandTile): Tile = mb.band(0)
    /** Get the green band. */
    def green(mb: MultibandTile): Tile = mb.band(1)
    /** Get the blue band. */
    def blue(mb: MultibandTile): Tile = mb.band(2)

    /** Convert the tile to an Int-based cell type. */
    def normalizeCellType(tile: Tile): Tile = tile.convert(IntCellType)

    /** Convert tile such that cells values fall between 0 and 255. */
    def compressRange(tile: Tile): Tile = tile

    /** Apply color correction so it "looks nice". */
    def colorAdjust(tile: Tile): Tile = {
      val pipeline =
        brightnessCorrect(brightness) _ andThen
        clampByte andThen
        gammaCorrect(gamma) andThen
        clampByte andThen
        contrastCorrect(contrast) andThen
        clampByte

      normalizeCellType(tile).map(pipeline)
    }

    val applyAdjustment = compressRange _ andThen colorAdjust
  }
  case object Default extends Profile

  case object Landsat8NaturalColor extends Profile {
    // @lossyrob magic numbers: "Fiddled with until visually it looked ok. ¯\_(ツ)_/¯"
    override val brightness = 15
    override val contrast = 30
    override val gamma = 0.8
    val (clampMin, clampMax) = (4000, 15176)

    override def compressRange(tile: Tile): Tile = {
      val clamper = clamp(clampMin, clampMax) _
      tile.map(clamper).normalize(clampMin, clampMax, 0, 255)
    }
  }

  case object NAIPNaturalColor extends Profile {
    override val gamma = 1.4
    override def compressRange(tile: Tile): Tile = tile.rescale(0, 255)
  }

  def rgbComposite(tile: MultibandTile, profile: Profile): Png = {
    val red = profile.applyAdjustment(profile.red(tile))
    val green = profile.applyAdjustment(profile.green(tile))
    val blue = profile.applyAdjustment(profile.blue(tile))
    ArrayMultibandTile(red, green, blue).renderPng
  }
}
