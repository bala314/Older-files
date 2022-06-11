import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.types.{IntegerType, StructField, StructType,StringType}
import org.apache.spark.sql.Row
import rdd_operations.spark

object unstructure extends App {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("analyse the unstructure data")
      .master("local[*]")
      .getOrCreate()

/*    val raw_df = spark.read.textFile("file:///C://Users/balu/desktop/Inputs/html.txt")
    raw_df.show()

  import spark.implicits._

  val extract_data = raw_df.select(regexp_extract($"value","""^.*://\w+.\w+.\w+/\w{6}\S[a-z]\S\w+\S\w+\S\d\w+\S\w+\S(\w+)""",1) as "content")
  extract_data.show()*/

  val raw_data = spark.sparkContext.textFile("file:///C://Users/balu/desktop/Inputs/questions_10k.txt")
raw_data.foreach(println)
  println(raw_data.count())

  val spl_data = raw_data.map(x => x.split(","))
  //spl_data.collect.foreach(arr => println(arr.toList))
val new_data = spl_data.filter(_.length==7)
  println("no of recors befor filtering is : "+spl_data.count())
  println("no of records after filtering is : " +new_data.count())
  new_data.collect.foreach(arr => println(arr.toList))

  import spark.implicits._
  /*val df1 = new_data.toDF()
  df1.show()*/

/*  val scheema = StructType(
    StructField(("id",IntegerType,false),("certificationDate",StringType,false),("closedate",StringType,false),
    ("deletiondatw",StringType,false),("score",IntegerType,false),("ownerUserId",IntegerType,false),
    ("answercount",IntegerType,false))::Nil
  )

  val rowRdd = new_data.map( line => Row(line))
  val df1 = spark.createDataFrame(rowRdd,scheema)*/

 case class scheema(id:Int,certificationdate:String,closedate:String,deletiondate:String,Sscore:String,OwnerUserId:String,AnswerCount:String)

val df1 = new_data.map(x => scheema(x(0).trim.toInt,x(1),x(2),x(3),x(4),x(5),x(6))).toDF()
  df1.show()
  df1.printSchema()
}