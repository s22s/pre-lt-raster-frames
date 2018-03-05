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

import java.io.UnsupportedEncodingException
import java.net.URI
import java.sql.Timestamp
import java.time.{ZoneOffset, ZonedDateTime}

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.geotrellis.TileFeatureSupport._
import astraea.spark.rasterframes.jts.SpatialFilters.{BetweenTimes, Contains ⇒ sfContains, Intersects ⇒ sfIntersects}
import astraea.spark.rasterframes.datasource.geotrellis.GeoTrellisRelation._
import astraea.spark.rasterframes.util._
import com.vividsolutions.jts.geom
import geotrellis.raster._
import geotrellis.raster.merge.TileMergeMethods
import geotrellis.raster.prototype.TilePrototypeMethods
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.tiling.Tiler.Options
import geotrellis.spark.tiling._
import geotrellis.spark.util.KryoWrapper
import geotrellis.util.{LazyLogging, MethodExtensions}
import geotrellis.vector._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.jts.PolygonUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * A Spark SQL `Relation` over a standard GeoTrellis layer.
 */
case class GeoTrellisRelation(sqlContext: SQLContext,
                              uri: URI,
                              layerId: LayerId,
                              numPartitions: Option[Int] = None,
                              failOnUnrecognizedFilter: Boolean = false,
                              filters: Seq[Filter] = Seq.empty,
                              subdivideTile: Option[Int] = None)
    extends BaseRelation with PrunedScan with LazyLogging {

  implicit val sc = sqlContext.sparkContext

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
        case c if c.isAssignableFrom(classOf[MultibandTile]) ⇒ typeOf[MultibandTile]
        case c if c.isAssignableFrom(classOf[TileFeature[_, _]]) ⇒ typeOf[TileFeature[Tile, _]]
        case c ⇒ throw new UnsupportedOperationException("Unsupported tile type " + c)
      }
      (kt, tt)
    })


  @transient
  lazy val tileLayerMetadata: Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]] = {

    val subdivide = subdivideTile.getOrElse(1)
    def subdivideTLM[T](tlm: TileLayerMetadata[T]): TileLayerMetadata[T] = tlm.copy(layout = newLayout(tlm.layout))

    def newLayout(l: LayoutDefinition): LayoutDefinition = if(subdivideTile.isEmpty) l
    else {
      require(subdivide > 1, "subdivideTile parameter must be greater than 1.")
      require(l.tileLayout.tileRows % subdivide == 0, s"Tile size must be divisible by subdivideTile parameter: ${l.tileLayout.tileRows} rows not divisible by $subdivide.")
      require(l.tileLayout.tileCols % subdivide == 0, s"Tile size must be divisible by subdivideTile parameter: ${l.tileLayout.tileCols} columns not divisible by $subdivide.")
      val tl = TileLayout(
        layoutRows = l.tileLayout.layoutRows * subdivide,
        layoutCols = l.tileLayout.layoutCols * subdivide,
        tileCols = l.tileLayout.tileCols / subdivide,
        tileRows = l.tileLayout.tileRows / subdivide
      )
      l.copy(tileLayout = tl)
    }

    keyType match {
      case t if t =:= typeOf[SpaceTimeKey] ⇒ Right(
        subdivideTLM[SpaceTimeKey](attributes.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId))
      )
      case t if t =:= typeOf[SpatialKey] ⇒ Left(
        subdivideTLM[SpatialKey](attributes.readMetadata[TileLayerMetadata[SpatialKey]](layerId))
      )
    }
  }

  private object Cols {
    lazy val SK = SPATIAL_KEY_COLUMN.columnName
    lazy val TK = TEMPORAL_KEY_COLUMN.columnName
    lazy val TS = TIMESTAMP_COLUMN.columnName
    lazy val TL = TILE_COLUMN.columnName
    lazy val TF = TILE_FEATURE_DATA_COLUMN.columnName
    lazy val EX = BOUNDS_COLUMN.columnName
  }

  /** This unfortunate routine is here because the number bands in a  multiband layer isn't written
   * in the metadata anywhere. This is potentially an expensive hack, which needs further quantifying of impact.
   * Another option is to force the user to specify the number of bands. */
  private lazy val peekBandCount = {
    tileClass match {
      case t if t =:= typeOf[MultibandTile] ⇒
        val reader = keyType match {
          case k if k =:= typeOf[SpatialKey] ⇒
            LayerReader(uri).read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
          case k if k =:= typeOf[SpaceTimeKey] ⇒
            LayerReader(uri).read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
        }
        // We're counting on `first` to read a minimal amount of data.
        val tile = reader.first()
        tile._2.bandCount
      case _ ⇒ 1
    }
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
      case t if t =:= typeOf[MultibandTile] ⇒
        for(b ← 1 to peekBandCount) yield
          StructField(Cols.TL + "_" + b, TileUDT, nullable = true)
      case t if t =:= typeOf[TileFeature[Tile, _]] ⇒
        List(
          StructField(Cols.TL, TileUDT, nullable = true),
          StructField(Cols.TF, DataTypes.StringType, nullable = true)
        )
    }

    val extentField = StructField(Cols.EX, PolygonUDT, false)
    StructType((keyFields :+ extentField) ++ tileFields)
  }

  type BLQ[K, T] = BoundLayerQuery[K, TileLayerMetadata[K], RDD[(K, T)] with geotrellis.spark.Metadata[TileLayerMetadata[K]]]

  def applyFilter[K: Boundable: SpatialComponent, T](query: BLQ[K, T], predicate: Filter): BLQ[K, T] = {
    predicate match {
      // GT limits disjunctions to a single type
      case sources.Or(sfIntersects(Cols.EX, left), sfIntersects(Cols.EX, right)) ⇒
        query.where(LayerFilter.Or(
          Intersects(Extent(left.getEnvelopeInternal)),
          Intersects(Extent(right.getEnvelopeInternal))
        ))
      case sfIntersects(Cols.EX, rhs: geom.Point) ⇒
        query.where(Contains(Point(rhs)))
      case sfContains(Cols.EX, rhs: geom.Point) ⇒
        query.where(Contains(Point(rhs)))
      case sfIntersects(Cols.EX, rhs) ⇒
        query.where(Intersects(Extent(rhs.getEnvelopeInternal)))
      case _ ⇒
        val msg = "Unable to convert filter into GeoTrellis query: " + predicate
        if(failOnUnrecognizedFilter)
          throw new UnsupportedOperationException(msg)
        else
          logger.warn(msg + ". Filtering defered to Spark.")
        query
    }
  }

  def applyFilterTemporal[K: Boundable: SpatialComponent: TemporalComponent, T](q: BLQ[K, T], predicate: Filter): BLQ[K, T] = {
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

    val reader = LayerReader(uri)

    val columnIndexes = requiredColumns.map(schema.fieldIndex)
    tileClass match {
      case t if t =:= typeOf[Tile] ⇒ query[Tile](reader, columnIndexes)
      case t if t =:= typeOf[TileFeature[Tile, _]] ⇒
        val baseSchema = attributes.readSchema(layerId)
        val schema = scala.util.Try(baseSchema
            .getField("pairs").schema()
            .getElementType
            .getField("_2").schema()
            .getField("data").schema()
        ).getOrElse(
          throw new UnsupportedEncodingException("Embedded TileFeature schema is of unknown/unexpected structure: " + baseSchema.toString(true))
        )
        implicit val codec = GeoTrellisRelation.tfDataCodec(KryoWrapper(schema))
        query[TileFeature[Tile, TileFeatureData]](reader, columnIndexes)
      case t if t =:= typeOf[MultibandTile] ⇒ query[MultibandTile](reader, columnIndexes)
    }
  }

  def query[T<: CellGrid: WithMergeMethods :WithPrototypeMethods: AvroRecordCodec: ClassTag]
  (reader: FilteringLayerReader[LayerId], columnIndexes: Seq[Int]): RDD[Row] = {

    val parts = numPartitions.getOrElse(reader.defaultNumPartitions)

    val newMapKeyTransform = tileLayerMetadata.fold(_.layout.mapTransform, _.layout.mapTransform)

    tileLayerMetadata.fold(
      // Without temporal key case
      (tlm: TileLayerMetadata[SpatialKey]) ⇒ {
        val query = splitFilters.foldLeft(
          reader.query[SpatialKey, T, TileLayerMetadata[SpatialKey]](layerId, parts)
        )(applyFilter(_, _))

        val rdd = query.result

        rdd
          .map { case (sk: SpatialKey, tile: T) ⇒

            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ newMapKeyTransform.keyToExtent(sk).jtsGeom
              case 2 ⇒ tile match {
                case t: Tile ⇒ t
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.tile
                case m: MultibandTile ⇒ m.bands.head
              }
              case i if i > 2 ⇒ tile match {
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.data
                case m: MultibandTile ⇒ m.bands(i - 2)
              }
            }
            Row(entries: _*)
          }
      }, // With temporal key case
      (tlm: TileLayerMetadata[SpaceTimeKey]) ⇒ {
        val query = splitFilters.foldLeft(
          reader.query[SpaceTimeKey, T, TileLayerMetadata[SpaceTimeKey]](layerId, parts)
        )(applyFilterTemporal(_, _))

        val rdd = query.result
        logger.debug(s"Query RDD has ${rdd.partitions.length}, parameter was ${numPartitions}")

        /* Note when there are both numPartitions and subdivideTile options
         *    we will change the partitioning to try to honor the numPartitions  */
        val subdividedRdd: RDD[(SpaceTimeKey, T)] = if(subdivideTile.isEmpty) rdd
        else rdd
          .mapPartitions { it ⇒
            it.map { case (k, v) ⇒
              (TemporalProjectedExtent(rdd.metadata.mapTransform(k), rdd.metadata.crs, k.instant), v)
            }
          }
          .tileToLayout(tlm, Options(NearestNeighbor, numPartitions.map(new HashPartitioner(_))))

        logger.debug(s"Subdivided RDD has ${rdd.partitions.length} partitions, parameter was `numPartitions`=${numPartitions} and `subdivideTile`=$subdivideTile")

       subdividedRdd
          .map { case (stk: SpaceTimeKey, tile: T) ⇒
            val sk = stk.spatialKey
            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ stk.temporalKey
              case 2 ⇒ new Timestamp(stk.temporalKey.instant)
              case 3 ⇒ newMapKeyTransform.keyToExtent(stk).jtsGeom
              case 4 ⇒ tile match {
                case t: Tile ⇒ t
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.tile
                case m: MultibandTile ⇒ m.bands.head
              }
              case i if i > 4 ⇒ tile match {
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.data
                case m: MultibandTile ⇒ m.bands(i - 4)
              }
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
  /** A dummy type used as a stand-in for ignored TileFeature data. */
  type TileFeatureData = String

  implicit object TileFeatureDataOps extends MergeableData[TileFeatureData] {
    override def merge(l: TileFeatureData, r: TileFeatureData): TileFeatureData = l + r

    override def prototype(data: TileFeatureData): TileFeatureData = ""
  }

  /** Constructor for Avro codec for TileFeature data stand-in. */
  private def tfDataCodec(dataSchema: KryoWrapper[Schema]) = new AvroRecordCodec[TileFeatureData]() {
    def schema: Schema = dataSchema.value
    def encode(thing: TileFeatureData, rec: GenericRecord): Unit = ()
    def decode(rec: GenericRecord): TileFeatureData = rec.toString
  }
}
