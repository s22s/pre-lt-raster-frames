/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2018 Azavea & Astraea, Inc.
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
import astraea.spark.rasterframes.expressions.SpatialExpression
import astraea.spark.rasterframes.util._
import geotrellis.proj4.LatLng
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, _}
import geotrellis.util.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.geomesa.curve._
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

import scala.reflect.runtime.universe._

/**
 * @author echeipesh
 * @author sfitch
 */
case class GeoTrellisRelation(sqlContext: SQLContext, uri: URI, layerId: LayerId)
    extends BaseRelation with PrunedFilteredScan with LazyLogging {

  @transient
  private implicit val spark = sqlContext.sparkSession

  @transient
  private lazy val attributes = AttributeStore(uri)

  @transient
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

  @transient
  lazy val tileLayerMetadata: Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]] =
    keyType match {
      case t if t =:= typeOf[SpaceTimeKey] ⇒ Right(
        attributes.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId)
      )
      case t if t =:= typeOf[SpatialKey] ⇒ Left(
        attributes.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
      )
    }

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

    val siMetadata = new MetadataBuilder().tagSpatialIndex.build()
    val indexField = StructField(SPATIAL_INDEX_COLUMN.columnName, LongType, nullable = false, siMetadata)
    StructType((indexField +: keyFields) ++ tileFields)
  }

  /** Declare filter handling. */
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case (_: IsNotNull | _: IsNull) => true
      case _ => false
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Reading: $layerId from $uri")
    logger.debug(s"Required columns: ${requiredColumns.toList}")
    logger.debug(s"PushedDown filters: ${filters.toList}")


    implicit val sc = sqlContext.sparkContext
    lazy val reader = LayerReader(uri)

    /**
     *   def mapTransform: MapKeyTransform = self.tileLayerMetadata.widen.mapTransform


     *     val transform = self.sparkSession.sparkContext.broadcast(mapTransform)
    val crs = self.tileLayerMetadata.widen.crs
    (r: Row) ⇒ {
      val center = transform.value.keyToExtent(SpatialKey(r.getInt(0), r.getInt(1))).center.reproject(crs, LatLng)
      (center.x, center.y)
    }
     *
     *     val zindex = udf(keyCol2LatLng andThen (p ⇒ Z2SFC.index(p._1, p._2).z))
    self.withColumn(colName, zindex(self.spatialKeyColumn))

     */

    val columnIndexes = requiredColumns.map(schema.fieldIndex)

    tileLayerMetadata.fold(
      // Without temporal key case
      (tlm: TileLayerMetadata[SpatialKey]) ⇒ {

        val key2Index = GeoTrellisRelation.XZ2Indexer(tlm)

        val rdd = reader
          .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
          .result

        rdd
          .map { case (sk: SpatialKey, tile: Tile) ⇒

            val entries = columnIndexes.map {
              case 0 ⇒ key2Index(sk)
              case 1 ⇒ sk
              case 2 ⇒ tile
            }
            Row(entries: _*)
          }
      }, // With temporal key case
      (tlm: TileLayerMetadata[SpaceTimeKey]) ⇒ {
        val key2Index = GeoTrellisRelation.XZ2Indexer(tlm)

        val rdd = reader
          .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
          .result

        rdd
          .map { case (stk: SpaceTimeKey, tile: Tile) ⇒
            val sk = stk.spatialKey
            val entries = columnIndexes.map {
              case 0 ⇒ key2Index(sk)
              case 1 ⇒ sk
              case 2 ⇒ stk.temporalKey
              case 3 ⇒ tile
            }
            Row(entries: _*)
          }
      }
    )
  }

  // TODO: Is there size speculation we can do?
  override def sizeInBytes = {
    super.sizeInBytes
  }
}

object GeoTrellisRelation {

  def XZ2Indexer(tlm: TileLayerMetadata[_]) = {
    val trans = tlm.mapTransform
    val crs = tlm.crs
    (sk: SpatialKey) ⇒ {
      val sfc = XZ2SFC(SpatialExpression.xzPrecision)
      trans.keyToExtent(sk).reproject(crs, LatLng) |>
        (e ⇒ sfc.index(e.xmin, e.ymin, e.xmax, e.ymax))
    }
  }

//  def XZ3Indexer(tlm: TileLayerMetadata[_]) = {
//    val trans = tlm.mapTransform
//    val crs = tlm.crs
//    (stk: SpaceTimeKey) ⇒ {
//      val sfc = XZ3SFC(SpatialExpression.xzPrecision, TimePeriod.Day)
//      trans.keyToExtent(stk.spatialKey).reproject(crs, LatLng) |>
//        (e ⇒ sfc.index(e.xmin, e.ymin, stk.instant, e.xmax, e.ymax, stk.instant))
//    }
//  }
}

