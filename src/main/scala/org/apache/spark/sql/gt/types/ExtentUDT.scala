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

import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, UserDefinedType}

/**
 * Catalyst representation of GT Extent.
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] class ExtentUDT extends UserDefinedType[Extent] {

  override val typeName = "st_extent"

  override val simpleString = typeName

  override val sqlType = StructType(Array(
    StructField("xmin", DoubleType, false),
    StructField("ymin", DoubleType, false),
    StructField("xmax", DoubleType, false),
    StructField("ymax", DoubleType, false)
  ))

  override def serialize(obj: Extent): Any = {
    InternalRow(obj.xmin, obj.ymin, obj.xmax, obj.ymax)
  }

  override def deserialize(datum: Any): Extent = {
    val row = datum.asInstanceOf[InternalRow]
    Extent(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3))
  }

  override def userClass: Class[Extent] = classOf[Extent]
}

object ExtentUDT extends ExtentUDT
