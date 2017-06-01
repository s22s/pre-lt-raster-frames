package org.apache.spark.sql.gt.types

import geotrellis.vector._
import org.apache.spark.sql.types.UDTRegistration

/**
 * UDT for GT vector polygon
 *
 * @author sfitch 
 * @since 5/11/17
 */
private[gt] class PointUDT extends AbstractGeometryUDT[Point]("st_point")

case object PointUDT extends PointUDT {
  UDTRegistration.register(classOf[Point].getName, classOf[PointUDT].getName)
}

private[gt] class LineUDT extends AbstractGeometryUDT[Line]("st_line")

case object LineUDT extends LineUDT {
  UDTRegistration.register(classOf[Line].getName, classOf[LineUDT].getName)
}

private[gt] class MultiLineUDT extends AbstractGeometryUDT[MultiLine]("st_multiline")

case object MultiLineUDT extends MultiLineUDT {
  UDTRegistration.register(classOf[MultiLine].getName, classOf[MultiLineUDT].getName)
}

private[gt] class PolygonUDT extends AbstractGeometryUDT[Polygon]("st_polygon")

case object PolygonUDT extends PolygonUDT {
  UDTRegistration.register(classOf[Polygon].getName, classOf[PolygonUDT].getName)
}
