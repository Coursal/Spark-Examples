import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object BankTransfers
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("BankTransfers").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val input_df = spark.read.option("header", "true").option("delimiter", " ")
          .csv("file://" + System.getProperty("user.dir") + "/bank_dataset/transfers.csv")
          .groupBy("Bank")
          .agg(count("Bank").as("Num of Transfers"), sum("Amount").as("Total"))
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/output")
    }
}
