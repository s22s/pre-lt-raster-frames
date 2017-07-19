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

import geotrellis.spark.Metadata
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *
 * @author sfitch 
 * @since 7/18/17
 */
abstract class ContextRDDMethods[K: ClassTag: TypeTag,
                                 V: TileComponent: ClassTag,
                                 M: JsonFormat: BoundsComponentOf[K]#get](implicit spark: SparkSession)
  extends MethodExtensions[RDD[(K, V)] with Metadata[M]] {

  private[rasterframes] implicit class WithMetadataMethods[M: JsonFormat](val self: M) extends MetadataMethods[M]

  def toRF: RasterFrame = {
    import spark.implicits._
    // Need to use this instead of `(v: V).getComponent[Tile]`
    // due to Spark Closure Cleaner error.
    val tileGetter = implicitly[TileComponent[V]]
    val md = self.metadata.asColumnMetadata
    (self: RDD[(K, V)])
      .mapValues(tileGetter.get)
      .toDF("key", "tile")
      .setColumnMetadata("key", md)
  }
}
