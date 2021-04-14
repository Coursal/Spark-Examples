import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.desc

object TopWords
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Top N Word Count").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

        val sc = spark.sparkContext

        // cleanup/split the text from the docs, map each word as (word, 1), and reduce by key
        val wordcount = sc.textFile("file:///path/to/project/folder/TopWords/metamorphosis/*")
          .flatMap(text => text.replaceAll("\\d+", "")
                                .replaceAll("[^a-zA-Z ]", " ")
                                .toLowerCase()
                                .trim()
                                .replaceAll("\\s+", " ")
                                .split(" "))
          .map(word => (word, 1))
          .reduceByKey(_ + _)

        val topN = spark.createDataFrame(wordcount).toDF("word", "wordcount")
          .orderBy(desc("wordcount"))      // sort words by their wordcount in descending order
          .limit(10)                       // take the first N == 10 words
          .repartition(1)                  // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file:///path/to/project/folder/TopWords/output")
    }
}
