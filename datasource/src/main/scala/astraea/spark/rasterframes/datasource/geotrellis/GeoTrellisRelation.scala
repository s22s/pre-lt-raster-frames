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
import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.SpatialFilters.{BetweenTimes, Contains ⇒ sfContains, Intersects ⇒ sfIntersects}
import astraea.spark.rasterframes.util._
import com.vividsolutions.jts.geom
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.{LayerId, SpatialKey, TileLayerMetadata, _}
import geotrellis.util.LazyLogging
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

import scala.reflect.runtime.universe._

/**
 * A Spark SQL `Relation` over a standard GeoTrellis layer.
 * @author echeipesh
 * @author sfitch
 */
case class GeoTrellisRelation(sqlContext: SQLContext, uri: URI, layerId: LayerId, filters: Seq[Filter] = Seq.empty)
    extends BaseRelation with PrunedScan with LazyLogging {

  /** Convenience to create new relation with the give filter added. */
  def withFilter(value: Filter): GeoTrellisRelation =
    copy(filters = filters :+ value)

  /** Separate And conditions into separate filters. */
  def splitFilters = {
    def splitConjunctives(f: Filter): Seq[Filter] =
    f match {
      case And(cond1, cond2) =>
        splitConjunctives(cond1) ++ splitConjunctives(cond2)
      case other => other :: Nil
    }
    filters.flatMap(splitConjunctives)
  }

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

  private object Cols {
    lazy val SK = SPATIAL_KEY_COLUMN.columnName
    lazy val TK = TEMPORAL_KEY_COLUMN.columnName
    lazy val TS = TIMESTAMP_COLUMN.columnName
    lazy val TL = TILE_COLUMN.columnName
    lazy val EX = EXTENT_COLUMN.columnName
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
          StructField(Cols.SK, skSchema, nullable = false, skMetadata),
          StructField(Cols.TK, tkSchema, nullable = false, tkMetadata),
          StructField(Cols.TS, TimestampType, nullable = false)
        )
      case t if t =:= typeOf[SpatialKey] ⇒
        List(
          StructField(Cols.SK, skSchema, nullable = false, skMetadata)
        )
    }

    val tileFields = tileClass match {
      case t if t =:= typeOf[Tile]  ⇒
        List(
          StructField(Cols.TL, TileUDT, nullable = true)
        )
    }

    val extentSchema = ExpressionEncoder[Extent]().schema
    val extentField = StructField(Cols.EX, extentSchema, false)
    StructType((keyFields :+ extentField) ++ tileFields)
  }

  type BLQ[K] = BoundLayerQuery[K, TileLayerMetadata[K], TileLayerRDD[K]]

  def applyFilter[K: Boundable: SpatialComponent](q: BLQ[K], predicate: Filter): BLQ[K] = {
    predicate match {
      // GT limits disjunctions to a single type
      case sources.Or(sfIntersects(Cols.EX, left), sfIntersects(Cols.EX, right)) ⇒
        q.where(LayerFilter.Or(
          Intersects(Extent(left.getEnvelopeInternal)),
          Intersects(Extent(right.getEnvelopeInternal))
        ))
      case sfIntersects(Cols.EX, rhs: geom.Point) ⇒
        q.where(Contains(Point(rhs)))
      case sfContains(Cols.EX, rhs: geom.Point) ⇒
        q.where(Contains(Point(rhs)))
      case sfIntersects(Cols.EX, rhs) ⇒
        q.where(Intersects(Extent(rhs.getEnvelopeInternal)))
    }
  }

  def applyFilterTemporal[K: Boundable: SpatialComponent: TemporalComponent](q: BLQ[K], predicate: Filter): BLQ[K] = {
    def toZDT(ts: Timestamp) = ZonedDateTime.ofInstant(ts.toInstant, ZoneOffset.UTC)
    predicate match {
      case sources.EqualTo(Cols.TS, ts: Timestamp) ⇒
        q.where(At(toZDT(ts)))
      case BetweenTimes(Cols.TS, start: Timestamp, end: Timestamp) ⇒
        q.where(Between(toZDT(start), toZDT(end)))
      case _ ⇒ applyFilter(q, predicate)
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logger.debug(s"Reading: $layerId from $uri")
    logger.trace(s"Required columns: ${requiredColumns.mkString(", ")}")
    logger.trace(s"Filters: $filters")

    implicit val sc = sqlContext.sparkContext
    lazy val reader = LayerReader(uri)

    val columnIndexes = requiredColumns.map(schema.fieldIndex)

    tileLayerMetadata.fold(
      // Without temporal key case
      (tlm: TileLayerMetadata[SpatialKey]) ⇒ {
        val trans = tlm.mapTransform

        val query = splitFilters.foldLeft(
          reader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
        )(applyFilter(_, _))

        val rdd = query.result

        rdd
          .map { case (sk: SpatialKey, tile: Tile) ⇒

            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ trans.keyToExtent(sk)
              case 2 ⇒ tile
            }
            Row(entries: _*)
          }
      }, // With temporal key case
      (tlm: TileLayerMetadata[SpaceTimeKey]) ⇒ {
        val trans = tlm.mapTransform

        val query = splitFilters.foldLeft(
          reader.query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId)
        )(applyFilterTemporal(_, _))

        val rdd = query.result

        rdd
          .map { case (stk: SpaceTimeKey, tile: Tile) ⇒
            val sk = stk.spatialKey
            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ stk.temporalKey
              case 2 ⇒ new Timestamp(stk.temporalKey.instant)
              case 3 ⇒ trans.keyToExtent(stk)
              case 4 ⇒ tile
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
}

