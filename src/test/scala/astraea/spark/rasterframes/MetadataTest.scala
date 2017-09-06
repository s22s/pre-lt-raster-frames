package astraea.spark.rasterframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.MetadataBuilder

/**
 * Test rig for column metadata management.
 *
 * @author sfitch 
 * @since 9/6/17
 */
class MetadataTest extends TestEnvironment with TestData  {
  import spark.implicits._

  private val sampleMetadata = new MetadataBuilder().putBoolean("haz", true).putLong("baz", 42).build()

  describe("Metadata storage") {
    it("should serialize and attach metadata") {
      //val rf = sampleGeoTiff.projectedRaster.toRF(128, 128)
      val df = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val withmeta = df.addColumnMetadata($"num", "stuff", sampleMetadata)

      val meta2 = withmeta.getColumnMetadata($"num", "stuff")
      assert(Some(sampleMetadata) === meta2)
    }

    it("should handle post-join duplicate column names") {
      val df1 = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val df2 = spark.createDataset(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("num", "str")
      val joined = df1.as("a").join(df2.as("b"), "num")

      joined.printSchema()
      joined.show()

      val withmeta = joined.addColumnMetadata(df1("str"), "stuff", sampleMetadata)
      val meta2 = withmeta.getColumnMetadata($"str", "stuff")

      assert(Some(sampleMetadata) === meta2)
    }
  }
}
