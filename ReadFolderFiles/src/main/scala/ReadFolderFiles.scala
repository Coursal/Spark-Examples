import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadFolderFiles
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Read Folder Files").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

		    val sc = spark.sparkContext

        // create an RDD where the full path of each file is the key and the file's content is the value,
        // and get rid of the full path of the file to only set the actual name of each file
        val input = sc.wholeTextFiles("file:///path/to/project/folder/ReadFolderFiles/alphabet_dir/*")
          .map(file => (file._1.split('/').last, file._2))

        // convert the RDD to a DataFrame and explicitly name the columns
        // while replacing the newlines `\n` with 3 dashes in the `content` column
        val input_df = spark.createDataFrame(input).toDF("filename", "content")
          .withColumn("content", regexp_replace(col("content"), "[\\r\\n]", " --- "))
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .csv("file:///path/to/project/folder/ReadFolderFiles/output")
    }
}
