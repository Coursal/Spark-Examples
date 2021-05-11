import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OldestTree
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Oldest Tree").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val oldest_tree = spark.read.option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/trees/trees.csv")
          .withColumn("age_of_tree",col("age_of_tree").cast(IntegerType))
          .orderBy(desc("age_of_tree"))
          .take(1)
          .toList
          .toString

        println(oldest_tree)
    }
}
