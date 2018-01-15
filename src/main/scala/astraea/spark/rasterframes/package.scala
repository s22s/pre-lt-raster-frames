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

import java.sql.Timestamp

import astraea.spark.rasterframes.encoders.GeoTrellisEncoders
import astraea.spark.rasterframes.jts.SpatialEncoders
import com.vividsolutions.jts.geom.{Point ⇒ jtsPoint}
import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark.{Bounds, ContextRDD, Metadata, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.util.GetComponent
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import shapeless.tag.@@
import shapeless.tag
import org.apache.spark.sql.functions._

/**
 *  Module providing support for RasterFrames.
 * `import astraea.spark.rasterframes._`., and then call `rfInit(SQLContext)`.
 *
 * @author sfitch
 * @since 7/18/17
 */
package object rasterframes extends ColumnFunctions
  with Implicits with jts.Implicits with GeoTrellisEncoders with SpatialEncoders {
  type Statistics = astraea.spark.rasterframes.functions.CellStatsAggregateFunction.Statistics
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._

  /**
   * Initialization injection point. Must be called before any RasterFrame
   * types are used.
   */
  @deprecated("Please use 'SparkSession.withRasterFrames' or 'SQLContext.withRasterFrames' instead.", "0.5.3")
  def rfInit(sqlContext: SQLContext): Unit = sqlContext.withRasterFrames

  /** Default RasterFrame spatial column name. */
  val SPATIAL_KEY_COLUMN = col("spatial_key").as[SpatialKey]

  /** Default RasterFrame temporal column name. */
  val TEMPORAL_KEY_COLUMN = col("temporal_key").as[TemporalKey]

  /** Default RasterFrame timestamp column name */
  val TIMESTAMP_COLUMN = col("timestamp").as[Timestamp]

  /** Default RasterFrame column name for an tile extent value. */
  val EXTENT_COLUMN = col("extent").as[Extent]

  /** Default RasterFrame column name for the center coordinates of the tile's extent. */
  // This is a `def` because `PointUDT` needs to be initialized first.
  def CENTER_COLUMN = col("center").as[jtsPoint]

  /** Default RasterFrame column name for an added spatial index. */
  val SPATIAL_INDEX_COLUMN = col("spatial_index").as[Long]

  /** Default RasterFrame tile column name. */
  // This is a `def` because `TileUDT` needs to be initialized first.
  def TILE_COLUMN = col("tile").as[Tile]

  /** Default RasterFrame [[TileFeature.data]] column name. */
  val TILE_FEATURE_DATA_COLUMN = col("tile_data")

  /** Default GeoTiff tags column. */
  val METADATA_COLUMN = col("metadata").as[Map[String, String]]

  /** Default column index column for the cells of exploded tiles. */
  val COLUMN_INDEX_COLUMN = col("column_index").as[Int]

  /** Default teil column index column for the cells of exploded tiles. */
  val ROW_INDEX_COLUMN = col("row_index").as[Int]

  /** Key under which ContextRDD metadata is stored. */
  private[rasterframes] val CONTEXT_METADATA_KEY = "_context"

  /** Key under which RasterFrame role a column plays. */
  private[rasterframes] val SPATIAL_ROLE_KEY = "_stRole"

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
  private[rasterframes] def certifyRasterframe(df: DataFrame): RasterFrame =
    tag[RasterFrameTag][DataFrame](df)

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type get[M] = GetComponent[M, Bounds[K]]
  }

  type TileFeatureLayerRDD[K, D] =
    RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]

  object TileFeatureLayerRDD {
    def apply[K, D](rdd: RDD[(K, TileFeature[Tile, D])],
      metadata: TileLayerMetadata[K]): TileFeatureLayerRDD[K, D] =
      new ContextRDD(rdd, metadata)
  }

  trait HasCellType[T] extends Serializable
  object HasCellType {
    implicit val intHasCellType = new HasCellType[Int] {}
    implicit val doubleHasCellType = new HasCellType[Double] {}
    implicit val byteHasCellType = new HasCellType[Byte] {}
    implicit val shortHasCellType = new HasCellType[Short] {}
    implicit val floatHasCellType = new HasCellType[Float] {}
  }
}
