package org.apache.spark.sql.gt.sources

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import java.net.URI

import geotrellis.spark._
import geotrellis.vector.Extent

class GeoTrellisDataSource extends DataSourceRegister with RelationProvider {
  def shortName(): String = "geotrellis"

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.contains("uri"), "'uri' parameter for catalog location required.")
    require(parameters.contains("layer"), "'layer' parameter for raster layer name required.")
    require(parameters.contains("zoom"), "'zoom' parameter for raster layer zoom level required.")

    gt.gtRegister(sqlContext)

    val uri: URI = URI.create(parameters("uri"))
    val layerId: LayerId = LayerId(parameters("layer"), parameters("zoom").toInt)
    val bbox: Option[Extent] = parameters.get("bbox").map(Extent.fromString)

    // here would be the place to read the layer metadata
    // and dispatch based on key type to spatial or spacetime relation
    GeoTrellisRelation(sqlContext, uri, layerId, bbox)
  }
}
