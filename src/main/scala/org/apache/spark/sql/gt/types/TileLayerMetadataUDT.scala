package org.apache.spark.sql.gt.types

import geotrellis.raster.CellType
import geotrellis.spark.{KeyBounds, SpaceTimeKey, TileLayerMetadata}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.gt.Implicits._
import org.apache.spark.sql.gt._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Catalyst representation of GT [[TileLayerMetadata]]. This is necessary
 * even though [[TileLayerMetadata]] is a case class because `cellType`
 * is declared with a intersection-based type alias and a UDT for that
 * hasn't been figured out.
 *
 * @author sfitch 
 * @since 7/20/17
 */
class TileLayerMetadataUDT extends UserDefinedType[TileLayerMetadata[SpaceTimeKey]] {
  override val typeName = "st_tilelayermetadata"

  def sqlType: DataType = StructType(Seq(
    StructField("cellType", StringType, false),
    StructField("layout", layoutDefinitionEncoder.schema, false),
    StructField("extent", extentEncoder.schema, false),
    StructField("crs", crsEncoder.schema, false),
    StructField("bounds", stkBoundsEncoder.schema, false)
  ))

  def serialize(obj: TileLayerMetadata[SpaceTimeKey]): Any = {
    Option(obj)
      .map(o ⇒ {
        InternalRow(
          UTF8String.fromString(o.cellType.name),
          layoutDefinitionEncoder.toRow(o.layout),
          extentEncoder.toRow(o.extent),
          crsEncoder.toRow(o.crs),
          stkBoundsEncoder.toRow(o.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]])
        )
      })
      .orNull
  }

  def deserialize(datum: Any): TileLayerMetadata[SpaceTimeKey] = {
    Option(datum)
      .collect { case row: InternalRow ⇒ row }
      .map(row ⇒ {
        TileLayerMetadata[SpaceTimeKey](
          CellType.fromName(row.getString(0)),
          layoutDefinitionEncoder.decode(row, 1),
          extentEncoder.decode(row, 2),
          crsEncoder.decode(row, 3),
          stkBoundsEncoder.decode(row, 4)
        )
      })
      .orNull
  }

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case o: TileLayerMetadataUDT  ⇒ o.typeName == this.typeName
    case _ ⇒ super.acceptsType(dataType)
  }

  def userClass: Class[TileLayerMetadata[SpaceTimeKey]] = classOf[TileLayerMetadata[SpaceTimeKey]]
}


case object TileLayerMetadataUDT extends TileLayerMetadataUDT {
  UDTRegistration.register(classOf[TileLayerMetadata[SpaceTimeKey]].getName, classOf[TileLayerMetadataUDT].getName)
}
