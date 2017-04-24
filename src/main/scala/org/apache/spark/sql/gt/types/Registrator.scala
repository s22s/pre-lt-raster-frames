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

import com.esotericsoftware.kryo.Kryo
import geotrellis.spark.util.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{UDTRegistration, UserDefinedType}
import org.apache.spark.serializer.KryoRegistrator

/**
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] object Registrator {

  val types = Seq(
    TileUDT,
    MultibandTileUDT,
    CoordinateReferenceSystemUDT,
    HistogramUDT
  )

  def register(implicit sqlContext: SQLContext): Unit = {
    types.foreach(register)
  }

  private def register(udt: UserDefinedType[_])(implicit ctx: SQLContext): Unit = {
    UDTRegistration.register(
      udt.userClass.getCanonicalName,
      udt.getClass.getSuperclass.getName
    )
  }
}
