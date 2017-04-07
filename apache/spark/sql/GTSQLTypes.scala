/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */

package org.apache.spark.sql

import com.vividsolutions.jts.{geom ⇒ jts}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.runtime.universe._

/**
 * Spark UDTs for Geotrellis
 *
 * @author sfitch
 * @since 4/3/17
 */
private[spark] object GTSQLTypes {
  def register(sqlContext: SQLContext): Unit = {
    register(TileUDT)
    register(MultibandTileUDT)
    register(ExtentUDT)
    register(ProjectedExtentUDT)
    register(TemporalProjectedExtentUDT)
  }

  private[spark] def register(udt: UserDefinedType[_]): Unit = {
    UDTRegistration.register(
      udt.userClass.getCanonicalName,
      udt.getClass.getSuperclass.getName
    )
  }

  private[spark] def runtimeClass[T: TypeTag]: Class[T] =
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  private[spark] abstract class GeoTrellisUDT[T >: Null: AvroRecordCodec: TypeTag]
  (override val typeName: String) extends UserDefinedType[T] {

    override val simpleString = typeName

    // TODO: Consider using built-in kryo encoder
    override def sqlType: StructType = StructType(Array(
      StructField(simpleString + "_avro", BinaryType)
    ))

    override def serialize(obj: T): InternalRow = {
      val bytes = AvroEncoder.toBinary[T](obj)
      InternalRow(bytes)
    }

    override def deserialize(datum: Any): T = {
      val row = datum.asInstanceOf[InternalRow]
      AvroEncoder.fromBinary[T](row.getBinary(0))
    }

    override def userClass: Class[T] = runtimeClass[T]
  }

  private [spark] class MultibandTileUDT extends GeoTrellisUDT[MultibandTile]("st_multibandtile")
  object MultibandTileUDT extends MultibandTileUDT

  private [spark] class TileUDT extends GeoTrellisUDT[Tile]("st_tile")
  object TileUDT extends TileUDT

  private [spark] class ExtentUDT extends UserDefinedType[Extent] {

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

  private [spark] class ProjectedExtentUDT extends UserDefinedType[ProjectedExtent] {

    override def typeName = "st_projectedextent"

    override def simpleString = typeName

    override def sqlType = StructType(Array(
      StructField("extent", ExtentUDT.sqlType, false),
      StructField("crs", StringType, false)
    ))

    override def serialize(obj: ProjectedExtent): Any = {
      val extent = ExtentUDT.serialize(obj.extent)
      val crs = UTF8String.fromString(obj.crs.toProj4String)
      InternalRow(extent, crs)
    }

    override def deserialize(datum: Any): ProjectedExtent = {
      val row = datum.asInstanceOf[InternalRow]
      val extent = ExtentUDT.deserialize(row.get(0, ExtentUDT.sqlType))
      val proj4 = row.getString(1)
      ProjectedExtent(extent, CRS.fromString(proj4))
    }

    override def userClass: Class[ProjectedExtent] = classOf[ProjectedExtent]
  }
  object ProjectedExtentUDT extends ProjectedExtentUDT

  private [spark] class TemporalProjectedExtentUDT extends UserDefinedType[TemporalProjectedExtent] {

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

}
