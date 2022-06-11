import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.Properties

object MysqlIntigration extends App{
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().
    appName("Intigration with mysql").
    master("local[*]").
    getOrCreate()

  val url = "jdbc:mysql://127.0.0.1:3306/relations"
  val table = "relations.series"
  val properties = new Properties()
  properties.put("user","Balu-PC")
  properties.put("password","mysql")

  Class.forName("com.mysql.jdbc.Driver")

  val mysqldf = spark.read.jdbc(url,table,properties)
  mysqldf.show()

}
