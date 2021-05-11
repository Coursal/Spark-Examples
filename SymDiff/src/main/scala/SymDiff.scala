import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SymDiff
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Symmetric Difference").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val input_df_A = spark.read.option("delimiter", " ")
          .csv("file://" + System.getProperty("user.dir") + "/input/A.txt")
          .select("_c0")    // only select the column with the ID's (the first one here)

        val input_df_B = spark.read.option("delimiter", " ")
          .csv("file://" + System.getProperty("user.dir") + "/input/B.txt")
          .select("_c1")    // only select the column with the ID's (the second one here)

        // the symmetric difference of two sets is equal to their union WITHOUT their intersected elements
        input_df_A.unionAll(input_df_B).except(input_df_A.intersect(input_df_B))
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .csv("file://" + System.getProperty("user.dir") + "/output")
    }
}
