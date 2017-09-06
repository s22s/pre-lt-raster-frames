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

private[gt] class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]("st_multipolygon")

case object MultiPolygonUDT extends MultiPolygonUDT {
  UDTRegistration.register(classOf[MultiPolygon].getName, classOf[MultiPolygonUDT].getName)
}
