package org.apache.spark.sql.gt.types

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{UDTRegistration, UserDefinedType}

/**
 *
 * @author sfitch 
 * @since 4/12/17
 */
private[gt] object Registrar {
  def register(sqlContext: SQLContext): Unit = {
    register(TileUDT)
    register(MultibandTileUDT)
    register(ExtentUDT)
    register(ProjectedExtentUDT)
    register(TemporalProjectedExtentUDT)
  }

  private def register(udt: UserDefinedType[_]): Unit = {
    UDTRegistration.register(
      udt.userClass.getCanonicalName,
      udt.getClass.getSuperclass.getName
    )
  }
}
