import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NormGrades
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Normalise Grades").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        // read the input csv file and cast the "Grade" column with integer values
        val input_df = spark.read.option("header", "true")
          .csv("file:///path/to/project/folder/NormGrades/grades/grades.csv")
          .withColumn("Grade", col("Grade").cast(IntegerType))

        // find max and min grades and isolate them to get their values
        val min_max_df = input_df.agg(min("Grade"), max("Grade")).head()

        val min_grade = min_max_df.getInt(0)
        val max_grade = min_max_df.getInt(1)

        // normalize the grades
        val norm_grades_df = input_df
          .withColumn("NormGrade", (col("Grade") - min_grade) / ((max_grade - min_grade)))
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file:///path/to/project/folder/NormGrades/output")
    }
}
