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

import astraea.spark.rasterframes.util.registerOptimization
import geotrellis.spark._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

/**
 * DataSource over a GeoTrellis layer store.
 *
 * @author echeipesh
 * @author sfitch
 */
@Experimental
class DefaultSource extends DataSourceRegister with RelationProvider {
  def shortName(): String = "geotrellis"

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.contains("path"), "'path' parameter required.")
    require(parameters.contains("layer"), "'layer' parameter for raster layer name required.")
    require(parameters.contains("zoom"), "'zoom' parameter for raster layer zoom level required.")

    registerOptimization(sqlContext, SpatialFilterPushdownRules)

    val uri: URI = URI.create(parameters("path"))
    val layerId: LayerId = LayerId(parameters("layer"), parameters("zoom").toInt)

    GeoTrellisRelation(sqlContext, uri, layerId)
  }
}
