import java.util.Properties

import org.apache.spark.sql.SparkSession

object MysqlConnection {
  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("Intigration with mysql")
      .master("local")
      .getOrCreate()

     val url = "jdbc:mysql://127.0.0.1:3306"
     val table = "world.city"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","mysql")

    Class.forName("com.mysql.jdbc.Driver")

    val mysqldf = spark.read.jdbc(url,table,properties)
    mysqldf.show()
  }

}
