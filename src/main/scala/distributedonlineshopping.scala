import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object distributedonlineshopping {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("DistributedOnlineShopping")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", true)
      .load("./src/main/resources/newdata.csv")

    val rdd = df.rdd

    rdd.take(10).foreach(println)

  }

}
