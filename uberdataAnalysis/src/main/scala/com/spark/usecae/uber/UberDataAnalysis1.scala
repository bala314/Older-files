package com.spark.usecae.uber

import org.apache.spark.sql.SparkSession

object UberDataAnalysis1 {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("analysing the uber data")
      .master("local")
      .getOrCreate()

  /*  val r1 = spark.sparkContext.textFile("file:///C://Users/Balu/Desktop/yellow_tripdata_2016-01.csv")
    println("no of records before filtering " +r1.count)
   r1.take(10).foreach(println)

    val data = r1.filter { x => {
      if (x.split(",").length >= 19)
        true
      else
        false
    }
    }.map(a => a.split(","))

    println("no of records after filtering " +data.count)
     */

    val data1 = spark.read.option("header","true").csv("file:///C://Users/Balu/Desktop/yellow_tripdata_2016-01.csv")
    //data1.show(10)
    val fildata =  data1.filter("passenger_count > 5 and total_amount > 30")
   // println(fildata.count)
    val grpdata = data1.groupBy("passenger_count")
    println(grpdata.count.show)
    // data1.select("passenger_count","total_amount").show(10)
  }

}
