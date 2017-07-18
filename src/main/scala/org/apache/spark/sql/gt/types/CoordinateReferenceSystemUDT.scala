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
import org.apache.spark.sql.types.{DataType, StringType, UDTRegistration, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Catalyst representation of GT CRS.
 *
 * @author sfitch
 * @since 4/24/17
 */
class CoordinateReferenceSystemUDT extends UserDefinedType[CRS] {
  override def sqlType: DataType = StringType

  override def serialize(obj: CRS): Any = {
    UTF8String.fromString(obj.toProj4String)
  }

  override def deserialize(datum: Any): CRS = {
    val proj4 = datum.asInstanceOf[UTF8String]
    CRS.fromString(proj4.toString)
  }

  override def userClass: Class[CRS] = classOf[CRS]
}
object CoordinateReferenceSystemUDT extends CoordinateReferenceSystemUDT {
  UDTRegistration.register(classOf[CRS].getName, classOf[CoordinateReferenceSystemUDT].getName)
}
