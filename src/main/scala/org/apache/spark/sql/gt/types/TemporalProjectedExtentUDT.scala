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

import geotrellis.proj4.CRS
import geotrellis.spark.TemporalProjectedExtent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Catalyst representation of GT TemporalProjectedExtent.
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] class TemporalProjectedExtentUDT extends UserDefinedType[TemporalProjectedExtent] {

  override def typeName = "st_temporalprojectedextent"

  override def simpleString = typeName

  override def sqlType = StructType(Array(
    StructField("extent", ExtentUDT.sqlType, false),
    StructField("crs", StringType, false),
    StructField("time", TimestampType, false)
  ))

  override def serialize(obj: TemporalProjectedExtent): Any = {
    val extent = ExtentUDT.serialize(obj.extent)
    val crs = UTF8String.fromString(obj.crs.toProj4String)
    val time = obj.instant * 1000
    InternalRow(extent, crs, time)
  }

  override def deserialize(datum: Any): TemporalProjectedExtent = {
    val row = datum.asInstanceOf[InternalRow]
    val extent = ExtentUDT.deserialize(row.get(0, ExtentUDT.sqlType))
    val proj4 = row.getString(1)
    val time = row.getLong(2) / 1000
    TemporalProjectedExtent(extent, CRS.fromString(proj4), time)
  }

  override def userClass: Class[TemporalProjectedExtent] = classOf[TemporalProjectedExtent]
}

object TemporalProjectedExtentUDT extends TemporalProjectedExtentUDT
