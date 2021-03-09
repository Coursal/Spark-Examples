import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AvgPrice
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Score Computation").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()
        
        val input_df = spark.read.option("header", "true")
          .csv("file:///path/to/project/folder/AvgPrice/address_book/address_book.csv")
          .withColumn("Price", col("Price").cast(DoubleType))
          .groupBy("Zipcode")
          .agg(avg("Price").as("AvgPrice"))
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file:///path/to/project/folder/AvgPrice/output")
    }
}
