/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package astraea.spark.rasterframes

import geotrellis.raster.{ProjectedRaster, Tile, TileFeature}
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialComponent, TileLayerMetadata}
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.types.{MetadataBuilder, Metadata ⇒ SMetadata}
import shapeless.Lub
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Library-wide implicit class definitions.
 *
 * @since 12/21/17
 */
trait Implicits {
  implicit class WithSparkSessionMethods(val self: SparkSession) extends SparkSessionMethods

  implicit class WithSQLContextMethods(val self: SQLContext) extends SQLContextMethods

  implicit class WithProjectedRasterMethods(val self: ProjectedRaster[Tile])
      extends ProjectedRasterMethods

  implicit class WithDataFrameMethods(val self: DataFrame) extends DataFrameMethods

  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods

  implicit class WithSpatialContextRDDMethods[K: SpatialComponent: JsonFormat: TypeTag](
    val self: RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]]
  )(implicit spark: SparkSession)
      extends SpatialContextRDDMethods[K]

  implicit class WithSpatioTemporalContextRDDMethods(
    val self: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  )(implicit spark: SparkSession)
      extends SpatioTemporalContextRDDMethods

  implicit class WithTFContextRDDMethods[K: SpatialComponent: JsonFormat: ClassTag: TypeTag,
                                         D: TypeTag](
    val self: RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]
  )(implicit spark: SparkSession)
      extends TFContextRDDMethods[K, D]

  implicit class WithTFSTContextRDDMethods[D: TypeTag](
    val self: RDD[(SpaceTimeKey, TileFeature[Tile, D])] with Metadata[
      TileLayerMetadata[SpaceTimeKey]]
  )(implicit spark: SparkSession)
      extends TFSTContextRDDMethods[D]

  private[astraea] implicit class WithMetadataMethods[R: JsonFormat](val self: R)
      extends MetadataMethods[R]

  private[astraea] implicit class WithMetadataAppendMethods(val self: SMetadata)
      extends MethodExtensions[SMetadata] {
    def append = new MetadataBuilder().withMetadata(self)
  }

  private[astraea] implicit class WithMetadataBuilderMethods(val self: MetadataBuilder)
      extends MetadataBuilderMethods

  private[astraea] implicit class WithWiden[A, B](thing: Either[A, B]) {

    /** Returns the value as a LUB of the Left & Right items. */
    def widen[Out](implicit ev: Lub[A, B, Out]): Out =
      thing.fold(identity, identity).asInstanceOf[Out]
  }

  private[astraea] implicit class WithCombine[T](left: Option[T]) {
    def combine[A, R >: A](a: A)(f: (T, A) ⇒ R): R = left.map(f(_, a)).getOrElse(a)
    def tupleWith[R](right: Option[R]): Option[(T, R)] = left.flatMap(l ⇒ right.map((l, _)))
  }

  implicit class NamedColumn(col: Column) {
    def columnName: String = col.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }
}
