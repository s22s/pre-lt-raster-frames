/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */

package org.apache.spark.sql.gt

import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark.{SpaceTimeKey, SpatialKey, TemporalProjectedExtent}
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.vectortile.protobuf.ProtobufTile
import org.apache.commons.io.IOUtils

import scala.reflect.ClassTag
import scala.util.Random

/**
 *
 * @author sfitch 
 * @since 4/3/17
 */
trait TestData {
  val instant = ZonedDateTime.now()
  val extent = Extent(1, 2, 3, 4)
  val sk = SpatialKey(37, 41)
  val stk = SpaceTimeKey(sk, instant)
  val pe = ProjectedExtent(extent, LatLng)
  val tpe = TemporalProjectedExtent(pe, instant)

  val byteArrayTile = ByteArrayTile((1 to 9).map(_ .toByte).toArray, 3, 3)

  val bitConstantTile = BitConstantTile(1, 2, 2)
  val byteConstantTile = ByteConstantTile(7, 3, 3)

  val multibandTile = MultibandTile(byteArrayTile, byteConstantTile)

  val allTileTypes: Seq[Tile] = {
    val rows = 3
    val cols = 3
    val range = 1 to rows * cols
    def rangeArray[T: ClassTag](conv: (Int ⇒ T)): Array[T] = range.map(conv).toArray
    Seq(
      BitArrayTile(Array[Byte](0,1,2,3,4,5,6,7,8), 3*8, 3),
      ByteArrayTile(rangeArray(_.toByte), rows, cols),
      DoubleArrayTile(rangeArray(_.toDouble), rows, cols),
      FloatArrayTile(rangeArray(_.toFloat), rows, cols),
      IntArrayTile(rangeArray(identity), rows, cols),
      ShortArrayTile(rangeArray(_.toShort), rows, cols),
      UByteArrayTile(rangeArray(_.toByte), rows, cols),
      UShortArrayTile(rangeArray(_.toShort), rows, cols)
    )
  }


}
