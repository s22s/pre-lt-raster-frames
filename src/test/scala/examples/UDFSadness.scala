package examples

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object UDFSadness extends App {
  implicit val spark = SparkSession.builder()
    .master("local").appName(getClass.getName).getOrCreate()
  import spark.implicits._

  case class KV(key: Long, value: String)
  case class MyRow(kv: KV)

  val ds: Dataset[MyRow] = spark.createDataset(List(MyRow(KV(1L, "a")), MyRow(KV(5L, "b"))))

  val firstColumn = ds(ds.columns.head)

  // Works, but is not what we want (can't always use `map` over `select`)
  ds.map(_.kv.value).show
  // +-----+
  // |value|
  // +-----+
  // |    a|
  // |    b|
  // +-----+

  // This is what we want to be able to implement
  val udf1 = udf((row: MyRow) ⇒ row.kv.value)

  try {
    ds.select(udf1(firstColumn)).show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // Exception in thread "main" org.apache.spark.sql.AnalysisException:
    // cannot resolve 'UDF(kv)' due to data type mismatch: argument 1 requires
    // struct<kv:struct<key:bigint,value:string>> type, however,
    // '`kv`' is of struct<key:bigint,value:string> type.;;
  }

  // So lets try something of the form reported in the error
  val udf2 = udf((kv: KV) ⇒ kv.value)

  try {
    ds.select(udf2(firstColumn)).show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    // cannot be cast to examples.UDFSadness$KV
  }

  // What if it's a problem with the use of untyped columns?
  // Try the above again with typed columns.

  try {
    ds.select(udf1(firstColumn.as[MyRow])).show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // org.apache.spark.sql.AnalysisException: cannot resolve 'UDF(kv)' due to data type
    // mismatch: argument 1 requires struct<kv:struct<key:bigint,value:string>> type,
    // however, '`kv`' is of struct<key:bigint,value:string> type.;;
  }

  try {
    ds.select(udf2(firstColumn.as[KV])).show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    // cannot be cast to examples.UDFSadness$KV

  }

  // Let's see if we can use SQL as a back door.
  ds.createOrReplaceTempView("myKVs")
  spark.sqlContext.udf.register("udf1", (row: MyRow) ⇒ row.kv.value)
  try {
    spark.sql(s"select udf1($firstColumn) from myKVs").show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // org.apache.spark.sql.AnalysisException: cannot resolve 'UDF(kv)' due to data
    // type mismatch: argument 1 requires struct<kv:struct<key:bigint,value:string>> type,
    // however, 'mykvs.`kv`' is of struct<key:bigint,value:string> type.; line 1 pos 7;
  }

  spark.sqlContext.udf.register("udf2", (kv: KV) ⇒ kv.value)
  try {
    spark.sql(s"select udf2($firstColumn) from myKVs").show
  }
  catch {
    case t: Throwable ⇒ t.printStackTrace()
    // java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    // cannot be cast to examples.UDFSadness$KV
  }

  // This is the unfortunate workaround. Note that `Row` is
  // `org.apache.spark.sql.Row`
  val udf3 = udf((row: Row) ⇒ row.getString(1))

  ds.select(udf3(firstColumn)).show
  //  +-------+
  //  |UDF(kv)|
  //  +-------+
  //  |      a|
  //  |      b|
  //  +-------+

  // As a coda, it's interesting to note that you can *return* a `Product` type from
  // a UDF, and the conversion works fine:

  val litValue = udf(() ⇒ KV(4l, "four"))

  ds.select(litValue()).show
  //  +--------+
  //  |   UDF()|
  //  +--------+
  //  |[4,four]|
  //  |[4,four]|
  //  +--------+

  println(ds.select(litValue().as[KV]).first)
  // KV(4,four)

}
