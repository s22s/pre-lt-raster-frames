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

import astraea.spark.rasterframes.datasource.geotrellis.GeoTrellisCatalog.GeoTrellisCatalogRelation
import geotrellis.spark.io.AttributeStore
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json.JsValue

/**
 *
 * @author sfitch 
 * @since 1/12/18
 */
@Experimental
class GeoTrellisCatalog extends DataSourceRegister with RelationProvider {
  def shortName() = "geotrellis-catalog"

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    require(parameters.contains("path"), "'path' parameter required.")
    val uri: URI = URI.create(parameters("path"))
    GeoTrellisCatalogRelation(sqlContext, uri)
  }
}

object GeoTrellisCatalog {
  case class GeoTrellisCatalogRelation(sqlContext: SQLContext, uri: URI) extends BaseRelation with TableScan {
    import sqlContext.implicits._

    private lazy val attributes = AttributeStore(uri)

    lazy val layerIds = attributes.layerIds
    // NB: It is expected that the number of layers is going to be small enough that
    // doing all this in-core generation of the catalog table will be negligible.
    // If this becomes a problem then re-write it starting off with an RDD of layerIds
    // and flow from there.
    lazy val layers = layerIds.toDF
    lazy val headers = sqlContext.read.json(
      layerIds
        .map(attributes.readHeader[JsValue])
        .map(_.compactPrint)
        .toDS
    )
    lazy val metadata = sqlContext.read.json(
      layerIds
        .map(attributes.readMetadata[JsValue])
        .map(_.compactPrint)
        .toDS
    )

    def schema: StructType = StructType(
      layers.schema.fields ++ headers.schema.fields ++ metadata.schema.fields
    )

    def buildScan(): RDD[Row] = {
      layers.rdd.zip(headers.rdd).zip(metadata.rdd).map { case ((l, h), m) ⇒
          val elements = l.toSeq ++ h.toSeq ++ m.toSeq
          Row(elements: _*)
      }
    }
  }
}
