/*
 * Copyright 2017 Azavea, Inc.
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

import geotrellis.spark.util.KryoSerializer
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** Catalyst representation of a bounding box */
class ExtentUDT extends UserDefinedType[Extent] {

  override val typeName = "st_extent"

  override def sqlType: DataType = StructType(List(
    StructField("xmin", DataTypes.DoubleType, nullable =false),
    StructField("ymin", DataTypes.DoubleType, nullable =false),
    StructField("xmax", DataTypes.DoubleType, nullable =false),
    StructField("ymax", DataTypes.DoubleType, nullable =false)
  ))

  override def serialize(obj: Extent): Any = {
    Option(obj)
      .map({ e => InternalRow(e.xmin, e.ymin, e.xmax, e.ymax)})
      .orNull
  }

  override def deserialize(datum: Any): Extent = {
    val row = datum.asInstanceOf[InternalRow]
    Extent(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
  }

  private[sql] override def acceptsType(dataType: DataType) = {
    dataType match {
      case o: ExtentUDT ⇒ o.typeName == this.typeName
      case _ ⇒ super.acceptsType(dataType)
    }
  }

  override def userClass: Class[Extent] = classOf[Extent]
}
object ExtentUDT extends ExtentUDT
