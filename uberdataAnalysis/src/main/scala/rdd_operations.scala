import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object rdd_operations extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

val spark = SparkSession.builder()
  .appName("rdd trasformations")
  .master("local[*]")
  .getOrCreate()

val raw_data = spark.sparkContext.textFile("file:///C://users/balu/desktop/Inputs/UniInputData.log")
  // raw_data.foreach(println)
  val newdata = raw_data.map(x => x.split(","))
 println(newdata.collect.foreach(arr => println(arr.toList)))

  println(raw_data.count())
 val matching = newdata.filter(_.length == 7)
  println(matching)
  println(matching.count())

  case class new_schema(id:Int,cert_date:String,close_date:String,delte_date:String,score:Int,owner_id:Int,asw_count:Int)

   import spark.implicits._

  //val newdf = newdata.toDF()

 val newdf = matching.map(x => new_schema(x(0).trim.toInt,x(1),x(2),x(3),x(4).trim.toInt,x(5).trim.toInt,x(6).trim.toInt)).toDF
  newdf.show()

 import spark.implicits._
 /* val unirdd = spark.sparkContext.textFile("file:///C://users/balu/desktop/Inputs/UniInputData.log")
 val counts = unirdd.filter{
   x => {if(x.toString().split(",").length >= 5)
   true
   else false
   }
 }.map(line => {line.toString().split(",")})*/


/*  def printall(nums: Int*){println("classes are "+nums.getClass)}

  printall(1,2,3)*/


/*val df = spark.read.option("header","true").csv("C:\\Users\\Balu\\Desktop\\Inputs\\explode.txt")
  df.show()
  val newdf = df.withColumn("newname",split($"name"," "))
    .select($"newname".getItem(0).as("fname"),$"newname".getItem(1).as("midname"),
    $"newname".getItem(2).as("lastname")).drop("newname")
  newdf.show()*/

  /* val df2 = newdf.withColumn("gender",when (col("fname") ==="bala" ,"male").
     when(col("fname") === "harpreet",lit("female")).otherwise("unknown"))
  df2.show()*/
  /*val df2 = newdf.withColumn("gender",when($"fname" === "bala","male")
 .when($"fname" === "harpreet","female"))
  df2.show()*/
}