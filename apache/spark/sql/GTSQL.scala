/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */

package org.apache.spark.sql

import geotrellis.raster.{MultibandTile, Tile, TileFeature}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Module providing support for using GeoTrellis native types in Spark SQL.
 * To use call [[GTSQL.init(SQLContext)]] and then `import GTSQL.Implicits._`.
 *
 * @author sfitch 
 * @since 3/30/17
 */
object GTSQL {
  def init(sqlContext: SQLContext): Unit = {
    GTSQLTypes.register(sqlContext)
    GTSQLFunctions.register(sqlContext)
  }

  trait Implicits {
    implicit def singlebandTileEncoder: Encoder[Tile] = ExpressionEncoder()
    implicit def multibandTileEncoder: Encoder[MultibandTile] = ExpressionEncoder()
    implicit def extentEncoder: Encoder[Extent] = ExpressionEncoder()
    implicit def projectedExtentEncoder: Encoder[ProjectedExtent] = ExpressionEncoder()
    implicit def temporalProjectedExtentEncoder: Encoder[TemporalProjectedExtent] = ExpressionEncoder()
  }
  object Implicits extends Implicits
}
