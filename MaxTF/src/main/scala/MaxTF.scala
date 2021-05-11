import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object MaxTF
{
    def main(args: Array[String]): Unit =
    {
        val conf = new SparkConf().setAppName("Maximum Term Frequency").setMaster("local")

        val spark = SparkSession.builder.config(conf).getOrCreate()

	val sc = spark.sparkContext

        // create an RDD where the full path of each doc is the key and the doc's content is the value,
        // and get rid of the full path of the doc to only set the actual name of each file,
        // while cleaning up the doc's text
        val input = sc.wholeTextFiles("file://" + System.getProperty("user.dir") + "/metamorphosis/*")
          .map(file => (file._1.split('/').last, file._2.replaceAll("\\d+", "")
                                                        .replaceAll("[^a-zA-Z ]", " ")
                                                        .toLowerCase()
                                                        .trim()
                                                        .replaceAll("\\s+", " ")))

        // group by words and documents to find the word count of each term
        val tf = spark.createDataFrame(input).toDF("doc", "text")
          .withColumn("doc_len", size(split(col("text"), " ")))
          .withColumn("words", explode(split(col("text"), " ")))
          .groupBy("doc", "words")
          // count each doc's length aka the number of terms (distinct words) each doc has
          .agg(count("words"),
              collect_set("doc_len")(0).as("doc_len"))  // set with 1 element, take the 1st element
          // calculate the term frequency of each word per doc
          .withColumn("tf", col("count(words)") / col("doc_len"))

        val max_tf = tf.groupBy("words") // group by words
          .agg(collect_list("doc").as("doc"),
              collect_list("tf").as("tf"),
              count("doc"))     // count the number of docs each word is found in them
          // create a MapType column `doc_tf` with a doc's name as key and the term frequency of each word in said doc
          .withColumn("doc_tf", map_from_arrays(col("doc"), col("tf")))
          // isolate and find the maximum term frequency score in `doc_tf`'s values for each word
          .withColumn("doc_tf_values", map_values(col("doc_tf")))
          .withColumn("max_tf", array_max(col("doc_tf_values")))
          // find the index of the doc-tf key-value pair with the maximum term frequency for each word
          .withColumn("max_position",
              array_position(col("doc_tf_values"), col("max_tf")))
          // isolate the keys of the `doc_tf` pairs and find the key/doc with the maximum term frequency for each word
          .withColumn("doc_tf_keys", map_keys(col("doc_tf")))
          .withColumn("max_tf_doc", col("doc_tf_keys")(col("max_position")-1))
          // drop unnecessary/in-between columns
          .drop("doc", "tf", "doc_tf", "doc_tf_keys", "doc_tf_values", "max_tf", "max_position")
          .repartition(1)   // save output to a single file
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("file://" + System.getProperty("user.dir") + "/output")
    }
}
