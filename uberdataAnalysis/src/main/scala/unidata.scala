import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date,col,unix_timestamp,from_unixtime}

object unidata extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("unidata")
    .master("local[*]")
    .getOrCreate()

  val df1 = spark.read.option("inferSchema","true").option("dateFormat","yyyy-mm-dd").csv("file:///C://Users/balu/desktop/Inputs/UniInputData.log")
      .toDF("product_id","product_name","seller","price","pur_date")
  df1.show()
  df1.printSchema()
  println(df1.columns.length)
  import spark.implicits._

 // val tempdf = df1.select("product_id","product_name","seller","price",from_unixtime(unix_timestamp($"pur_date","dd-MM-yyyy")).as("date"))

  /*  val df2 = df1.selectExpr("product_id","product_name","seller","price","cast(pur_date as date)")
df2.printSchema()
  df2.show()*/

  /*val newdf = df1.withColumn("newdate",to_date($"pur_date","dd-MM-yyyy"))
      .select("product_id","product_name","seller","price","newdate")
  newdf.show()
  newdf.printSchema()

  val pivotdf = newdf.groupBy("product_name").pivot("seller").sum("price").show()
  val pivotdf1 = newdf.groupBy("seller").pivot("product_name").sum("price").show()
  val pivotdf2 = newdf.groupBy("product_name","seller").sum("price").show()*/

}
