package astraea.spark.rasterframes

import geotrellis.spark.Metadata
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *
 * @author sfitch 
 * @since 7/18/17
 */
abstract class ContextRDDMethods[K: ClassTag: TypeTag,
                                 V: TileComponent: ClassTag,
                                 M: JsonFormat: BoundsComponentOf[K]#get](implicit spark: SparkSession)
  extends MethodExtensions[RDD[(K, V)] with Metadata[M]] {
  def toRF: RasterFrame = {
    import spark.implicits._
    val tileGetter = implicitly[TileComponent[V]]
    val md = self.metadata.asColumnMetadata
    (self: RDD[(K, V)]).mapValues(tileGetter.get)
      .toDF
      .select($"_1" as ("key", md), $"_2" as "tile")
  }
}
