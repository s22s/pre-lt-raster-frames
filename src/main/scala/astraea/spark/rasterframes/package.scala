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

package astraea.spark


import geotrellis.raster.{ProjectedRaster, Tile, TileFeature}
import geotrellis.spark.{Bounds, ContextRDD, Metadata, SpatialComponent, TileLayerMetadata}
import geotrellis.util.GetComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.gt.Implicits
import org.apache.spark.sql.gt.functions.ColumnFunctions
import spray.json.JsonFormat

import scala.reflect.runtime.universe._
import shapeless.tag
import shapeless.tag.@@

/**
 *  Module providing support for RasterFrames.
 * `import astraea.spark.rasterframes._`., and then call `rfInit(SQLContext)`.
 *
 * @author sfitch
 * @since 7/18/17
 */
package object rasterframes extends Implicits with ColumnFunctions {
  /** Key under which ContextRDD metadata is stored. */
  val CONTEXT_METADATA_KEY = "context"
  /** Default RasterFrame spatial column name. */
  val SPATIAL_KEY_COLUMN = "spatial_key"
  /** Default RasterFrame temporal column name. */
  val TEMPORAL_KEY_COLUMN = "temporal_key"
  /** Default RasterFrame tile column name. */
  val TILE_COLUMN = "tile"
  /** Default RasterFrame [[TileFeature.data]] column name. */
  val TILE_FEATURE_DATA_COLUMN = "tile_data"

  /**
   * A RasterFrame is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a [[geotrellis.spark.SpatialKey]] or [[geotrellis.spark.SpaceTimeKey]]
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrame = DataFrame @@ RasterFrameTag

  /** Tagged type for allowing compiler to help keep track of what has RasterFrame assurances applied to it. */
  trait RasterFrameTag

  /** Internal method for slapping the RasterFreame seal of approval on a DataFrame. */
  private[rasterframes] def certifyRasterframe(df: DataFrame): RasterFrame = tag[RasterFrameTag][DataFrame](df)

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type get[M] = GetComponent[M, Bounds[K]]
  }

  /**
   * Initialization injection point.
   */
  def rfInit(sqlContext: SQLContext): Unit = {
    // TODO: Can this be automatically done via some SPI-like construct in Spark?
    gt.gtRegister(sqlContext)
  }

  implicit class WithProjectedRasterMethods(val self: ProjectedRaster[Tile]) extends ProjectedRasterMethods
  implicit class WithDataFrameMethods(val self: DataFrame) extends DataFrameMethods
  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods
  implicit class WithContextRDDMethods[K: SpatialComponent: JsonFormat: TypeTag](val self: RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]])
    (implicit spark: SparkSession) extends ContextRDDMethods[K]

  implicit class WithTFContextRDDMethods[
    K: SpatialComponent: JsonFormat: TypeTag,
    D: TypeTag
  ](val self: RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]])
    (implicit spark: SparkSession) extends TFContextRDDMethods[K, D]

  type TileFeatureLayerRDD[K, D] = RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]
  object TileFeatureLayerRDD {
    def apply[K, D](rdd: RDD[(K, TileFeature[Tile, D])], metadata: TileLayerMetadata[K]): TileFeatureLayerRDD[K,D] =
      new ContextRDD(rdd, metadata)
  }

  private[astraea] implicit class WithMetadataMethods[R: JsonFormat](val self: R) extends MetadataMethods[R]
}
