import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Json_data extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("analysing Json data")
    .master("local[*]")
    .getOrCreate()

  val json_df = spark.read.format("json").option("inferSchema","true").option("header","true")
    .option("dateFormat","yyyy-MM-dd HH-mm-ss").load("file:///C://users/balu/desktop/Inputs/JsonData.json")

  json_df.show()
  json_df.printSchema()

  import spark.implicits._

val jsondf2 = json_df.select($"instid",$"instname",$"inststatus",explode(json_df("pathdet")) as "pathdet")
  jsondf2.printSchema()
   jsondf2.show()

  val newjson_df = jsondf2.select($"instid",$"instname",$"inststatus",$"pathdet.pinstid",$"pathdet.workitemid",
  $"pathdet.username",$"pathdet.docid",$"pathdet.startdate",$"pathdet.enddate")
  newjson_df.show()
  newjson_df.printSchema()

  val scheema = newjson_df.columns
  scheema.foreach(println)
}