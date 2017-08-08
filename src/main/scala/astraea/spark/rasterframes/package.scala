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


import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark.{Bounds, ContextRDD, Metadata, TileLayerMetadata}
import geotrellis.util.GetComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.gt.Implicits
import org.apache.spark.sql.gt.functions.ColumnFunctions
import spray.json.JsonFormat

import scala.reflect.runtime.universe._

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
  val SPATIAL_KEY_COLUMN = "key"
  val TILE_COLUMN = "tile"
  val TILE_FEATURE_DATA_COLUMN = "tile_data"

  /**
   * A RasterFrame is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a [[geotrellis.spark.SpatialKey]] or [[geotrellis.spark.SpaceTimeKey]]
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrame = DataFrame

  /** Initialization injection point. */
  def rfInit(sqlContext: SQLContext): Unit = {
    gt.gtRegister(sqlContext)
  }

  implicit class WithDataFrameMethods(val self: DataFrame) extends DataFrameMethods
  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods

  implicit class WithContextRDDMethods[
    K: TypeTag,
    M: JsonFormat: BoundsComponentOf[K]#get
  ](val self: RDD[(K, Tile)] with Metadata[M])(implicit spark: SparkSession) extends ContextRDDMethods[K,M]

  implicit class WithTFContextRDDMethods[
    K: TypeTag,
    D: TypeTag,
    M: JsonFormat: BoundsComponentOf[K]#get
  ](val self: RDD[(K, TileFeature[Tile, D])] with Metadata[M])(implicit spark: SparkSession) extends TFContextRDDMethods[K, D, M]

  type BoundsComponentOf[K] = {
    type get[M] = GetComponent[M, Bounds[K]]
  }

  type TileFeatureLayerRDD[K, D] = RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]
  object TileFeatureLayerRDD {
    def apply[K, D](rdd: RDD[(K, TileFeature[Tile, D])], metadata: TileLayerMetadata[K]): TileFeatureLayerRDD[K,D] =  new ContextRDD(rdd, metadata)
  }

  private[rasterframes] implicit class WithMetadataMethods[R: JsonFormat](val self: R) extends MetadataMethods[R]
}
