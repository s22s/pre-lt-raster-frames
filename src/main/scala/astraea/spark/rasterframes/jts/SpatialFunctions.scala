package astraea.spark.rasterframes.jts

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.jts._
import org.apache.spark.sql.{Column, Encoder, TypedColumn}
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral

/**
 * Spatial type support functions.
 *
 * @since 2/19/18
 */
trait SpatialFunctions {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._

  def geomlit(geom: Geometry): TypedColumn[Any, _ <: Geometry] = {
    def udtlit[T >: Null <: Geometry: Encoder, U <: AbstractGeometryUDT[T]](t: T, u: U): TypedColumn[Any, T] =
      new Column(GeometryLiteral(u.serialize(t), t)).as[T]

    geom match {
      case g: Point ⇒ udtlit(g, PointUDT)
      case g: LineString ⇒ udtlit(g, LineStringUDT)
      case g: Polygon ⇒ udtlit(g, PolygonUDT)
      case g: MultiPoint ⇒ udtlit(g, MultiPointUDT)
      case g: MultiLineString ⇒ udtlit(g, MultiLineStringUDT)
      case g: MultiPolygon ⇒ udtlit(g, MultiPolygonUDT)
      case g: GeometryCollection ⇒ udtlit(g, GeometryCollectionUDT)
      case g: Geometry ⇒ udtlit(g, GeometryUDT)
    }
  }
}
