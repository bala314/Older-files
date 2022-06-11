import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object frequency extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("rdd trasformations")
    .master("local[*]")
    .getOrCreate()

  val lst = spark.sparkContext.parallelize(List(1,2,3,1,1,2,1))
  val lst2 = lst.map(x => (x,1)).reduceByKey(_+_).mapValues(x => x+100).foreach(println)
}
