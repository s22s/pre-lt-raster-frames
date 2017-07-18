package astraea.spark.rasterframes
import geotrellis.util.MethodExtensions
import spray.json.{JsObject, JsonFormat}
import org.apache.spark.sql.types.{Metadata ⇒ SQLMetadata}

/**
 * Extension methods used for transforming the metadata in a ContextRDD.
 *
 * @author sfitch 
 * @since 7/18/17
 */
abstract class MetadataMethods[M: JsonFormat] extends MethodExtensions[M] {
  def asColumnMetadata: SQLMetadata = {
    val fmt = implicitly[JsonFormat[M]]
    fmt.write(self) match {
      case s: JsObject ⇒ SQLMetadata.fromJson(s.compactPrint)
      case _ ⇒ SQLMetadata.empty
    }
  }
}

