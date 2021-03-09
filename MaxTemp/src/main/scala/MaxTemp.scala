import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MaxTemp
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Max Temperature").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val input_df = spark.read.option("header", "true").option("delimiter", ", ")
          .csv("file:///path/to/project/folder/MaxTemp/temperatures/temperatures.csv")
          .groupBy("City")
          .agg(collect_list("Temperature").as("Temp_list"),
              max("Temperature").as("MaxTemp"))
          .drop("Temp_list")
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file:///path/to/project/folder/MaxTemp/output")
    }
}
