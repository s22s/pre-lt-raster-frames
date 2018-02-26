package astraea.spark.rasterframes.datasource.geotrellis

import astraea.spark.rasterframes.datasource.geotrellis.TileFeatureSupport._
import astraea.spark.rasterframes.{IntelliJPresentationCompilerHack, TestData, TestEnvironment}
import geotrellis.raster.crop.Crop
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleCellType, GridBounds, TileFeature, TileLayout}
import geotrellis.spark.tiling.Implicits._
import geotrellis.spark.{ContextRDD, TemporalProjectedExtent}
import geotrellis.vector.Extent
import org.scalatest.BeforeAndAfter


class TileFeatureSupportSpec extends TestEnvironment
  with TestData
  with BeforeAndAfter
  with IntelliJPresentationCompilerHack {

  val tf1 = TileFeature(squareIncrementingTile(3),"tf1")
  val tf2 = TileFeature(squareIncrementingTile(3),"tf2")
  val ext1 = Extent(10,10,20,20)
  val ext2 = Extent(15,15,25,25)

  describe("TileFeatureSupport") {
    it("should support merge with String data") {

      val merged = tf1.merge(tf2)
      assert(merged.tile == tf1.tile.merge(tf2.tile))
      assert(merged.data == "tf1tf2")

      assert(tf1.merge(ext1,ext2,tf2) == TileFeature(tf1.tile.merge(ext1,ext2,tf2.tile),"tf1tf2"))
      assert(tf1.merge(ext1,ext2,tf2,Bilinear) == TileFeature(tf1.tile.merge(ext1,ext2,tf2.tile,Bilinear),"tf1tf2"))
    }
    it("should support prototype with String data") {

      val proto = tf1.prototype(16,16)
      assert(proto.tile == byteArrayTile.prototype(16,16))
      assert(proto.data == "")

      assert(tf1.prototype(DoubleCellType,10,20) == tf1.prototype(DoubleCellType,10,20),"")
    }
    it("should support ops with custom data type") {

      case class Foo(num:Int,seq:Seq[String])

      implicit object FooOps extends DataOps[Foo] {
        override def merge(l: Foo, r: Foo): Foo = Foo(l.num + r.num, l.seq ++ r.seq)
        override def prototype(data: Foo): Foo = Foo(0,Seq())
      }

      val foo1 = Foo(10,Seq("Hello","Goodbye"))
      val foo2 = Foo(20, Seq("HelloAgain"))
      val fooTF1 = TileFeature(byteArrayTile,foo1)
      val fooTF2 = TileFeature(byteArrayTile,foo2)

      assert(fooTF1.merge(fooTF2) == TileFeature(byteArrayTile,Foo(30, Seq("Hello","Goodbye","HelloAgain"))))
      assert(fooTF1.prototype(20,20) == TileFeature(byteArrayTile.prototype(20,20),Foo(0,Seq())))
    }
    it("should enable tileToLayout over TileFeature RDDs") {

      // Create a RDD[(TemporalProjectedExtent,TileFeature[Tile,String])] suitable for tileToLayout
      val stkRDD = TestData.randomSpatioTemporalTileLayerRDD(20,20,10,10)
      val oldMD = stkRDD.metadata
      val peRDD = stkRDD.map{ case(k,v) => {
        val pe = TemporalProjectedExtent(oldMD.mapTransform(k),oldMD.crs,k.instant)
        val tf = TileFeature(v,"x")
        (pe,tf)
      }}

      // create a new tlm that will drive retile into RDD[(stk,tf)]
      val newLD = oldMD.layout.copy(tileLayout=TileLayout(5,5,40,40))
      val newMD = oldMD.copy(layout = newLD)

      // retile
      val newRDD = ContextRDD(peRDD.tileToLayout(newMD),newMD)
      assert(newRDD.count == 25)

      val firstTF = newRDD.first._2
      assert(firstTF.tile.rows == 40)
      assert(firstTF.tile.cols == 40)

    }
    it("should support crop operations") {

      // crop with extents
      val ext = Extent(0, 0, 100, 100)
      val ext2 = Extent(0, 0, 100, 50)
      val opts = Crop.Options.DEFAULT
      assert(tf1.crop(ext, ext2, opts) == TileFeature(tf1.tile.crop(ext, ext2, opts), tf1.data))

      // crop with gridbounds
      val gb = GridBounds(0,0,1,1)
      assert(tf1.crop(gb, opts) == TileFeature(tf1.tile.crop(gb, opts), tf1.data))
    }
    it("should support mask operations") {

      val ext = Extent(0, 0, 100, 100)
      val geoms = Seq(Extent(10,10,90,90).toPolygon())
      val opts = Rasterizer.Options.DEFAULT
      assert(tf1.mask(ext, geoms, opts) == TileFeature(tf1.tile.mask(ext, geoms, opts), tf1.data))

      val r = tf2.prototype(tf1.cols, tf1.rows)
      assert(tf1.localMask(r, 1, 2) == TileFeature(tf1.tile.localMask(r.tile, 1, 2), tf1.data))
      assert(tf1.localInverseMask(r, 1, 2) == TileFeature(tf1.tile.localInverseMask(r.tile, 1, 2), tf1.data))
    }
  }
}
