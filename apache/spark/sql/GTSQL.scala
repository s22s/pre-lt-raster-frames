package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{BitCellType, BitConstantTile, ByteCells, ByteConstantTile, CellType, DoubleCells, DoubleConstantTile, FloatCells, FloatConstantTile, IntCells, IntConstantTile, MultibandTile, ShortCells, ShortConstantTile, Tile, UByteCells, UByteConstantTile, UShortCells, UShortConstantTile}
import geotrellis.spark.io.avro.{AvroEncoder, AvroRecordCodec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 *
 * @author sfitch 
 * @since 3/30/17
 */
object GTSQL extends LazyLogging {

  def init(sqlContext: SQLContext): Unit = {
    import types._
    register(MultibandTileUDT)
    register(TileUDT)
    functions.register(sqlContext)
  }

  private[spark] def register(udt: GeoTrellisUDT[_]): Unit = {
    UDTRegistration.register(
      udt.userClass.getCanonicalName,
      udt.getClass.getSuperclass.getName
    )
  }

  abstract class GeoTrellisUDT[T >: Null: AvroRecordCodec: ClassTag](override val simpleString: String) extends UserDefinedType[T] {
    override def sqlType: DataType = StructType(Array(StructField("avro", BinaryType)))
    override def serialize(obj: T): InternalRow = {
      val bytes = AvroEncoder.toBinary[T](obj)
      new GenericInternalRow(Array[Any](bytes))
    }

    override def deserialize(datum: Any): T = {
      val row = datum.asInstanceOf[InternalRow]
      AvroEncoder.fromBinary[T](row.getBinary(0))
    }

    override def userClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  }

  private [spark] class MultibandTileUDT extends GeoTrellisUDT[MultibandTile]("multibandtile")
  object MultibandTileUDT extends MultibandTileUDT

  private [spark] class TileUDT extends GeoTrellisUDT[Tile]("tile")
  object TileUDT extends TileUDT


  object functions {
    val ST_MakeConstantTile: (Number, Int, Int, String) ⇒ Tile = (value, cols, rows, cellTypeName) ⇒ {
      val cellType = CellType.fromString(cellTypeName)
      cellType match {
        case BitCellType => BitConstantTile(if (value.intValue() == 0) false else true, cols, rows)
        case ct: ByteCells => ByteConstantTile(value.byteValue(), cols, rows, ct)
        case ct: UByteCells => UByteConstantTile(value.byteValue(), cols, rows, ct)
        case ct: ShortCells => ShortConstantTile(value.shortValue() , cols, rows, ct)
        case ct: UShortCells =>  UShortConstantTile(value.shortValue() , cols, rows, ct)
        case ct: IntCells =>  IntConstantTile(value.intValue() , cols, rows, ct)
        case ct: FloatCells => FloatConstantTile(value.floatValue() , cols, rows, ct)
        case ct: DoubleCells => DoubleConstantTile(value.doubleValue(), cols, rows, ct)
      }
    }

    def register(sqlContext: SQLContext): Unit = {
      sqlContext.udf.register("st_makeConstantTile", ST_MakeConstantTile)
    }

  }
}
