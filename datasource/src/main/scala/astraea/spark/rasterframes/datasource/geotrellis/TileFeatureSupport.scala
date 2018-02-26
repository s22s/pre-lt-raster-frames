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

  trait DataOps[D] {
    def merge(l:D, r:D): D
    def prototype(data:D): D
  }

  // To support other D types, extend DataOps as seen below:
  implicit object StringOps extends DataOps[String] {
    override def merge(l:String,r:String): String = s"$l $r"
    override def prototype(data:String): String = ""
  }

  implicit class TileFeatureMethodsWrapper[V <: CellGrid: ClassTag: WithMergeMethods: WithPrototypeMethods: WithCropMethods: WithMaskMethods, D:DataOps](val self: TileFeature[V, D])
    extends TileMergeMethods[TileFeature[V, D]]
      with TilePrototypeMethods[TileFeature[V,D]]
      with TileCropMethods[TileFeature[V,D]]
      with TileMaskMethods[TileFeature[V,D]]
      with MethodExtensions[TileFeature[V, D]] {

    override def merge(other: TileFeature[V, D]): TileFeature[V, D] =
      TileFeature(self.tile.merge(other.tile), implicitly[DataOps[D]].merge(self.data,other.data))

    override def merge(extent: Extent, otherExtent: Extent, other: TileFeature[V, D], method: ResampleMethod): TileFeature[V, D] =
      TileFeature(self.tile.merge(extent, otherExtent, other.tile, method), implicitly[DataOps[D]].merge(self.data,other.data))

    override def prototype(cols: Int, rows: Int): TileFeature[V, D] =
      TileFeature(self.tile.prototype(cols, rows), implicitly[DataOps[D]].prototype(self.data))

    override def prototype(cellType: CellType, cols: Int, rows: Int): TileFeature[V, D] =
      TileFeature(self.tile.prototype(cellType, cols, rows), implicitly[DataOps[D]].prototype(self.data))

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
