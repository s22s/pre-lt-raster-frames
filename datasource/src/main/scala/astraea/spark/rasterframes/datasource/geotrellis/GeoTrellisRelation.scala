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

import java.net.URI

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.geotrellis.SpatialFilterPushdownRules.FilterPredicate
import astraea.spark.rasterframes.util._
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, _}
import geotrellis.util.LazyLogging
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

import scala.reflect.runtime.universe._

/**
 * @author echeipesh
 * @author sfitch
 */
case class GeoTrellisRelation(sqlContext: SQLContext, uri: URI, layerId: LayerId, filters: Seq[FilterPredicate] = Seq.empty)
    extends BaseRelation with PrunedScan with LazyLogging {

  def withFilter(value: FilterPredicate): GeoTrellisRelation = copy(filters = filters :+ value)

  private implicit val spark = sqlContext.sparkSession

  private lazy val attributes = AttributeStore(uri)
  private lazy val (keyType, tileClass) = attributes.readHeader[LayerHeader](layerId) |>
    (h ⇒ {
      val kt = Class.forName(h.keyClass) match {
        case c if c.isAssignableFrom(classOf[SpaceTimeKey]) ⇒ typeOf[SpaceTimeKey]
        case c if c.isAssignableFrom(classOf[SpatialKey]) ⇒ typeOf[SpatialKey]
        case c ⇒ throw new UnsupportedOperationException("Unsupported key type " + c)
      }
      val tt = Class.forName(h.valueClass) match {
        case c if c.isAssignableFrom(classOf[Tile]) ⇒ typeOf[Tile]
        case c ⇒ throw new UnsupportedOperationException("Unsupported tile type " + c)
      }
      (kt, tt)
    })

  override def schema: StructType = {
    val skSchema = ExpressionEncoder[SpatialKey]().schema

    val skMetadata = attributes.readMetadata[JsValue](layerId) |>
      (m ⇒ Metadata.fromJson(m.compactPrint)) |>
      (Metadata.empty.append.attachContext(_).tagSpatialKey.build)

    val keyFields = keyType match {
      case t if t =:= typeOf[SpaceTimeKey] ⇒
        val tkSchema = ExpressionEncoder[TemporalKey]().schema
        val tkMetadata = Metadata.empty.append.tagTemporalKey.build
        List(
          StructField(SPATIAL_KEY_COLUMN.columnName, skSchema, nullable = false, skMetadata),
          StructField(TEMPORAL_KEY_COLUMN.columnName, tkSchema, nullable = false, tkMetadata)
        )
      case t if t =:= typeOf[SpatialKey] ⇒
        List(
          StructField(SPATIAL_KEY_COLUMN.columnName, skSchema, nullable = false, skMetadata)
        )
    }

    val tileFields = tileClass match {
      case t if t =:= typeOf[Tile]  ⇒
        List(
          StructField(TILE_COLUMN.columnName, TileUDT, nullable = true)
        )
    }

    val extentSchema = ExpressionEncoder[Extent]().schema
    val extentField = List(StructField(EXTENT_COLUMN.columnName, extentSchema, false))

    StructType(keyFields ++ extentField ++ tileFields)
  }


  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logger.debug(s"Reading: $layerId from $uri")
    logger.debug(s"Required columns: ${requiredColumns.toList}")

    implicit val sc = sqlContext.sparkContext
    lazy val reader = LayerReader(uri)

    val columnIndexes = requiredColumns.map(schema.fieldIndex)

    keyType match {
      // With temporal key case
      case k if k =:= typeOf[SpaceTimeKey] ⇒
          val rdd = reader
            .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
            .result

        val trans = rdd.metadata.layout.mapTransform
        rdd
          .map { case (stk: SpaceTimeKey, tile: Tile) ⇒
            val entries = columnIndexes.map {
              case 0 ⇒ stk.spatialKey
              case 1 ⇒ stk.temporalKey
              case 2 ⇒ trans(stk)
              case 3 ⇒ tile
            }
            Row(entries: _*)
          }
      // Without temporal key case
      case k if k =:= typeOf[SpatialKey] ⇒
        val rdd = reader
          .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          .result

        val trans = rdd.metadata.layout.mapTransform
        rdd
          .map { case (sk: SpatialKey, tile: Tile) ⇒
            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ trans(sk)
              case 2 ⇒ tile
            }

            Row(entries: _*)
          }
    }
  }

  // TODO: Is there size speculation we can do?
  override def sizeInBytes = {
    super.sizeInBytes
  }
}
