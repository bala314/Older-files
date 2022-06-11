import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
object MultiDelim {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("multiple delimiters")
      .master("local[*]").getOrCreate()

    val rawdf = spark.read.option("header","false").text("file:///C://Users/balu/Desktop/Inputs/Multidelim.txt")
    rawdf.show()

    val newdf = rawdf.select(split(col("value"),"\\~\\|").getItem(0),
      split(col("value"),"\\~\\|").getItem(1))    // splitign the data
    newdf.show()
    val firstRow = newdf.head  // Taking first row into a variable
    println(firstRow)

   val removeFirstRowdf = newdf.filter(x => x != firstRow)     // Removing first row from dataframe
    removeFirstRowdf.show()
    val actcol = firstRow.toSeq.asInstanceOf[Seq[String]]     //changing first row as a scheema
    println(actcol)
    val resdf = removeFirstRowdf.toDF(actcol:_*)             // adding that scheema to dataframe
    spark.time(resdf.show())
  }

}