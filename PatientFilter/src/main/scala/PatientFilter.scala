import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat_ws, col}

object PatientFilter
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Patient Filter").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        // create a new column named `CyclesAndCounseling` to concatenate the
        // `PatientCycleNum` and `Counseling` columns with a space separator
        // and use this new column to filter out the records with one cycle
        // and no counseling
        val input = spark.read.option("header", "true").option("delimiter", ", ")
          .csv("file://" + System.getProperty("user.dir") + "/patients/patients.csv")
          .withColumn("CyclesAndCounseling",
              concat_ws(" ", col("PatientCycleNum"), col("Counseling"))
                .as("CyclesAndCounseling"))
          .where("CyclesAndCounseling != '1 No'")
          .drop("CyclesAndCounseling")
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/output")
    }
}
