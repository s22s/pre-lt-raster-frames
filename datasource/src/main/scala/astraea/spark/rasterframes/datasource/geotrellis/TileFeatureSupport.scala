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

package astraea.spark.rasterframes.datasource.geotrellis

import geotrellis.raster.crop.{Crop, TileCropMethods}
import geotrellis.raster.mask.TileMaskMethods
import geotrellis.raster.merge.TileMergeMethods
import geotrellis.raster.prototype.TilePrototypeMethods
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellGrid, CellType, GridBounds, TileFeature}
import geotrellis.util.MethodExtensions
import geotrellis.vector.{Extent, Geometry}

import scala.reflect.ClassTag

object TileFeatureSupport {

  type WithMergeMethods[V] = (V => TileMergeMethods[V])
  type WithPrototypeMethods[V <: CellGrid] = (V => TilePrototypeMethods[V])
  type WithCropMethods[V <: CellGrid] = (V => TileCropMethods[V])
  type WithMaskMethods[V] = (V => TileMaskMethods[V])

  implicit class TileFeatureMethodsWrapper[V <: CellGrid: ClassTag: WithMergeMethods: WithPrototypeMethods: WithCropMethods: WithMaskMethods, D: MergeableData](val self: TileFeature[V, D])
    extends TileMergeMethods[TileFeature[V, D]]
      with TilePrototypeMethods[TileFeature[V,D]]
      with TileCropMethods[TileFeature[V,D]]
      with TileMaskMethods[TileFeature[V,D]]
      with MethodExtensions[TileFeature[V, D]] {

    override def merge(other: TileFeature[V, D]): TileFeature[V, D] =
      TileFeature(self.tile.merge(other.tile), MergeableData[D].merge(self.data,other.data))

    override def merge(extent: Extent, otherExtent: Extent, other: TileFeature[V, D], method: ResampleMethod): TileFeature[V, D] =
      TileFeature(self.tile.merge(extent, otherExtent, other.tile, method), MergeableData[D].merge(self.data,other.data))

    override def prototype(cols: Int, rows: Int): TileFeature[V, D] =
      TileFeature(self.tile.prototype(cols, rows), MergeableData[D].prototype(self.data))

    override def prototype(cellType: CellType, cols: Int, rows: Int): TileFeature[V, D] =
      TileFeature(self.tile.prototype(cellType, cols, rows), MergeableData[D].prototype(self.data))

    override def crop(srcExtent: Extent, extent: Extent, options: Crop.Options): TileFeature[V, D] =
      TileFeature(self.tile.crop(srcExtent, extent, options), self.data)

    override def crop(gb: GridBounds, options: Crop.Options): TileFeature[V, D] =
      TileFeature(self.tile.crop(gb, options), self.data)

    override def localMask(r: TileFeature[V, D], readMask: Int, writeMask: Int): TileFeature[V, D] =
      TileFeature(self.tile.localMask(r.tile, readMask, writeMask), self.data)

    override def localInverseMask(r: TileFeature[V, D], readMask: Int, writeMask: Int): TileFeature[V, D] =
      TileFeature(self.tile.localInverseMask(r.tile, readMask, writeMask), self.data)

    override def mask(ext: Extent, geoms: Traversable[Geometry], options: Rasterizer.Options): TileFeature[V, D] =
      TileFeature(self.tile.mask(ext, geoms, options), self.data)
  }
}

trait MergeableData[D] {
  def merge(l:D, r:D): D
  def prototype(data:D): D
}

object MergeableData {
  def apply[D: MergeableData]:MergeableData[D] = implicitly[MergeableData[D]]

  implicit def MergeableString: MergeableData[String] = new MergeableData[String] {
    override def merge(l:String,r:String): String = (l,r) match {
      case("",str:String) => str
      case(str:String,"") => str
      case(l:String,r:String) => s"$l, $r"
    }
    override def prototype(data:String): String = ""
  }

  implicit def mergeableSeq[T]: MergeableData[Seq[T]] = new MergeableData[Seq[T]] {
    override def merge(l: Seq[T], r: Seq[T]): Seq[T] = l ++ r
    override def prototype(data: Seq[T]): Seq[T] = Seq.empty
  }

  implicit def mergeableSet[T]: MergeableData[Set[T]] = new MergeableData[Set[T]] {
    override def merge(l: Set[T], r: Set[T]): Set[T] = l ++ r
    override def prototype(data: Set[T]): Set[T] = Set.empty
  }

  // to be used as the value in a mergeableMap, the data type, V, must have context bound MergeableData as well
  implicit def mergeableMap[K,V: MergeableData]: MergeableData[Map[K,V]] = new MergeableData[Map[K,V]] {
    override def merge(l: Map[K,V], r: Map[K,V]): Map[K,V] = {
      (l.toSeq ++ r.toSeq)
        .groupBy{case(k,v) => k}
        .mapValues(_.map(_._2)
                    .reduce[V] {case(lv,rv) => MergeableData[V].merge(lv,rv)})
    }
    override def prototype(data: Map[K,V]): Map[K,V] = Map.empty
  }
}

