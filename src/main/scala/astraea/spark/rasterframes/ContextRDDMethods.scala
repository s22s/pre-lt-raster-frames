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

package astraea.spark.rasterframes

import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spray.json.JsonFormat
import spray.json.DefaultJsonProtocol._

import scala.reflect.runtime.universe._

/**
 * Extension method on `ContextRDD`-shaped [[Tile]] RDDs with appropriate context bounds to create a RasterFrame.
 * @author sfitch 
 * @since 7/18/17
 */
abstract class ContextRDDMethods[K: SpatialComponent: JsonFormat: TypeTag](implicit spark: SparkSession)
  extends MethodExtensions[RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]]] {

  def toRF: RasterFrame = {
    import spark.implicits._
    val md = self.metadata.asColumnMetadata

    val rdd = self: RDD[(K, Tile)]
    rdd
      .toDF(SPATIAL_KEY_COLUMN, TILE_COLUMN)
      .addColumnMetadata(SPATIAL_KEY_COLUMN, CONTEXT_METADATA_KEY, md)
  }
}

/**
 * Extension method on `ContextRDD`-shaped [[TileFeature]] RDDs with appropriate context bounds to create a RasterFrame.
 * @author sfitch
 * @since 7/18/17
 */
abstract class TFContextRDDMethods[K: SpatialComponent: JsonFormat: TypeTag, D: TypeTag]
(implicit spark: SparkSession)
  extends MethodExtensions[RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]] {

  val TF_COL = "tileFeature"

  def toRF: RasterFrame = {
    import spark.implicits._
    val md = self.metadata.asColumnMetadata
    val rdd = self: RDD[(K, TileFeature[Tile, D])]

    rdd
      .toDF(SPATIAL_KEY_COLUMN, TF_COL)
      .addColumnMetadata(SPATIAL_KEY_COLUMN, CONTEXT_METADATA_KEY, md)
      .withColumn(TILE_COLUMN, $"$TF_COL.tile")
      .withColumn(TILE_FEATURE_DATA_COLUMN, $"$TF_COL.data")
      .drop(TF_COL)
  }
}
