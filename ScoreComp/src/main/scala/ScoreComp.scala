import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ScoreComp
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Score Computation").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        // read the input csv file and cast the "Num" column with double values, aggregate by character, calculate the
        // sum of the negative "Num" values and retrieve the single positive "Num value, before calculating the score
        // of each character based on the following expression: score == positive_num / (-1 * sum_of_negative_nums)
        val input_df = spark.read.option("header", "true").option("delimiter", " ")
          .csv("file:///path/to/project/folder/ScoreComp/input/input.csv")
          .withColumn("Num", col("Num").cast(DoubleType))
          .groupBy("Character")
          .agg(sum(when(col("Num") < 0, col("Num"))).as("NegSum"),
              max("Num").as("PosNum"))
          .withColumn("Score", col("PosNum") / (- col("NegSum")))
          .drop("NegSum", "PosNum")
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file:///path/to/project/folder/ScoreComp/output")
    }
}
