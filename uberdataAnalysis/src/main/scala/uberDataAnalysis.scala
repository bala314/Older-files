import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{unix_timestamp,to_date,to_timestamp,substring}

object uberDataAnalysis {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("analyse the uberdata")
      .master("local[*]")
      //.config("conf.ui.pory","12901")
      //.master("yarn")
      .getOrCreate()

    val rawdf = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C://Users/balu/desktop/Inputs/yellow_tripdata_2016-01.csv")
   // val rawdf = spark.read.format("csv").option("header","true").option("inferschema","true").load("/home/balasankar/inputs/yellow_tripdata_2016-01.csv")

    rawdf.show()
    rawdf.printSchema()

    import spark.implicits._

    val newdf = rawdf.withColumn("tpep_pickup_datetime",to_timestamp($"tpep_pickup_datetime","dd/MM/yyyy HH:mm"))
        .withColumn("tpep_dropoff_datetime",to_date($"tpep_dropoff_datetime","dd/MM/yyyy"))

    newdf.show()
    newdf.printSchema()
  }

}
