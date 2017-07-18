package astraea.spark


import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark.{Bounds, Metadata}
import geotrellis.util.{Component, GetComponent}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.gt.functions.ColumnFunctions
import org.apache.spark.sql.gt.{Implicits, gtRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, gt}
import spray.json.{JsObject, JsonFormat}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *  Module providing support for RasterFrames.
 * `import astraea.spark.rasterframes._`., and then call `rfInit(SQLContext)`.
 * @author sfitch 
 * @since 7/18/17
 */
package object rasterframes extends Implicits with ColumnFunctions {

  /**
   * A RasterFrame is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a [[geotrellis.spark.SpatialKey]] or [[geotrellis.spark.SpaceTimeKey]]
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrame = DataFrame

  type BoundsComponentOf[K] = {
    type get[M] = GetComponent[M, Bounds[K]]
  }

  type TileComponent[T] = GetComponent[T, Tile]

  /** Initialization injection point. */
  def rfInit(sqlContext: SQLContext): Unit = {
    gtRegister(sqlContext)
  }

  implicit class WithDataFrameMethods(val self: DataFrame) extends DataFrameMethods
  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods

  implicit class WithContextRDDMethods[
    K: ClassTag: TypeTag,
    V: TileComponent: ClassTag,
    M: JsonFormat: BoundsComponentOf[K]#get
  ](val self: RDD[(K, V)] with Metadata[M])(implicit spark: SparkSession)
  extends ContextRDDMethods[K,V,M]

  private[rasterframes]
  implicit class WithMetadataMethods[M: JsonFormat](val self: M) extends MetadataMethods[M]
}
