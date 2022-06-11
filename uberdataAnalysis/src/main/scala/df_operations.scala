import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{lit,typedLit,when}
//import org.apache.spark.rdd.RDD[AnyRef]

object df_operations extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("DATA FRAME OPERATIONS")
    .master("local[*]")
    .getOrCreate()

   // val df1 = spark.read.format("csv").option("inferSchema","true").option("header","true").load("file:///C://users/balu/desktop/Inputs/question_tags_10k.csv")

val df2 = spark.read.format("csv").option("inferSchema","true").option("header","true")
  .option("dateFormat","yyyy-MM-dd HH-mm-ss").load("file:///C://users/balu/desktop/Inputs/questions_10k.csv")


  /*df1.printSchema()
  df1.show()*/
  df2.printSchema()
df2.show()
  println("no of columns in the data frame " + df2.columns.length)

  df2.filter(df2("OwnerUserId").isNull).show()
  df2.filter(df2("OwnerUserId") === "NA").show()
  val newdf = df2.withColumn("OwnerUserId",col("OwnerUserId").cast("Int"))
  newdf.show()
 /* val badrecords = df2.filter("df2.isNull")
  badrecords.show()*/


  /*
  // Changing datatypes using cast

val df_questions = df2.select(df2.col("CloseDate").cast("timestamp"),
  df2.col("DeletionDate").cast("timestamp") as "deletion_date",
  df2.col("OwnerUserId").cast("integer"),df2.col("AnswerCount"))

  df_questions.printSchema();
  df_questions.show();

  // changing datatypes using sqlexpression

val df_questions = df2.selectExpr("id","CertificationDate",
  "cast(CloseDate as timestamp)","cast(DeletionDate as date) as DeletionDate","Score",
  "cast(OwnerUserId as integer) as OwnerUserId","AnswerCount")

  df_questions.printSchema();
  df_questions.show();


  // Changing datatypes using withcolumn

  val df_questions = df2.withColumn("Closed_Date",col("CloseDate").cast("timestamp")).drop("CloseDate")
    .withColumn("DeletionDate",col("DeletionDate").cast("date"))

  df_questions.printSchema()
  df_questions.show()


  //df_questions.withColumnRenamed("DeletionDate","deletedDate").show()
  df_questions.withColumn("AnswerCount",col("AnswerCount")+100).show()
  val new_df = df_questions.withColumn("Answer_Count",col("AnswerCount")* -1)
  new_df.show()
  new_df.drop("Answer_count").show()



df1.select("tag").show()
  df1.filter("tag == 'data'").show()

  // ADDING NEW COLUMNS TO DATA FRAME USING LIT

  val adding_column_df = df2.withColumn("rating",
    when(col("score") > 1000,lit(1)).otherwise(lit(2))
  )
    .select("id","CertificationDate","CloseDate","score","rating","AnswerCount")
  adding_column_df.printSchema()
  adding_column_df.show()


  val new_column = df1.select(col("id"),col("tag"),lit("newvalu").as("newvalue"))
new_column.show()

  import spark.implicits._
val new_value = List(1,2,4,5,6,3,8,7,9)
  val adding_new_values = adding_column_df.withColumn("rank",typedLit(new_value))
  adding_new_values.show()
*/

  // TO CHECK THE NULL AND NON NULL VALUES IN A FIELD

 /* println("null values owneruserid "+df2.filter(df2("OwnerUserId").isNull).count())
  println("null values in deletiondate "+df2.filter(df2("deletiondate").isNull || df2("deletiondate") === "NA").count())
  println("not null values in deletiondate "+df2.filter(df2("deletiondate").isNotNull && df2("deletiondate") =!= "NA").count())

  println(df2.isEmpty)*/

  // Droping the records which records have null values

  /*val drop_null_df = df2.na.drop()
  drop_null_df.show()*/

  // Droping the records if the column deletiondate has null

/*  val drop_col_null_df = df2.filter(df2("deletiondate").isNotNull).drop()
  drop_col_null_df.show()

  // Creating empty dataframe
  val empdf = spark.emptyDataFrame
  empdf.show()
println(empdf.isEmpty)*/


/*  println("null values owneruserid "+df2.filter(df2("OwnerUserId").isNull || df2("OwnerUserId") === "NA").count())

  import spark.implicits._

  println("null values in owneruserid "+df2.filter($"OwnerUserId".isNull || $"OwnerUserId" === "NA").count())
*/
/*  df2.na.drop().show()
  df2.na.drop("all").show()*/

}