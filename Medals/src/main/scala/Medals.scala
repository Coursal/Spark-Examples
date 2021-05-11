import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object Medals
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Medals").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val input_df = spark.read.option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/olympic_stats/athletes.csv")
          .groupBy("nationality", "sport")
          .agg(collect_list("gold").as("gold_list"),
              collect_list("silver").as("silver_list"),
              collect_list("bronze").as("bronze_list"),
              sum("gold").as("golds"),
              sum("silver").as("silvers"),
              sum("bronze").as("bronzes"))
          .drop("gold_list", "silver_list", "bronze_list")
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/output")
    }
}
