# Spark Examples
Some simple, kinda introductory projects based on Apache Spark to be used as guides in order to make the whole DataFrame data management look less weird or complex.

## Preparations & Prerequisites
* [Latest stable version](https://spark.apache.org/docs/latest/) of Spark or at least the one used here, [3.0.1](https://spark.apache.org/docs/3.0.1/).
*  A single node setup is enough. You can also use the applications in a local cluster or in a cloud service, with needed changes on anything to be parallelized, of course.
* Of course, having (a somehow recent version of) Scala (and Java) installed. 
* The most casual and convenient way to run the projects is to import them to a IDE as shown [here](https://sparkbyexamples.com/spark/spark-setup-run-with-scala-intellij/).

## Projects
Each project comes with its very own input data (`.csv` files in the project folder ready to be used or copied to the HDFS) and its execution results are either stored as a single file in an `/output` directory or printed in console. You should also set the full paths for the input and output folders for each project based on your system.

The projects featured in this repo are:

#### [AvgPrice](https://github.com/Coursal/Spark-Examples/tree/main/AvgPrice)
Calculating the average price of houses for sale by zipcode.

#### [BankTransfers](https://github.com/Coursal/Spark-Examples/tree/main/BankTransfers)
A typical "sum-it-up" example where for each bank we calculate the number and the sum of its transfers.

#### [MaxTemp](https://github.com/Coursal/Spark-Examples/tree/main/MaxTemp)
Typical case of finding the max recorded temperature for every city.

#### [Medals](https://github.com/Coursal/Spark-Examples/tree/main/Medals)
An interesting application of working on Olympic game stats  in order to see the total wins of gold, silver, and bronze medals of every athlete.

#### [NormGrades](https://github.com/Coursal/Spark-Examples/tree/main/NormGrades)
Just a plain old normalization example for a bunch of students and their grades.

#### [OldestTree](https://github.com/Coursal/Spark-Examples/tree/main/OldestTree)
Finding the oldest tree per city district. Child's play.

#### [ScoreComp](https://github.com/Coursal/Spark-Examples/tree/main/ScoreComp)
The most challenging and abstract one. Every key-character (`A`-`E`) has 3 numbers as values, two negatives and one positive. We just calculate the score for every character based on the following expression `character_score = pos / (-1 * (neg_1 + neg_2))`.

#### [SymDiff](https://github.com/Coursal/Spark-Examples/tree/main/SymDiff)
A simple way to calculate the symmetric difference between the records of two files, based on each record's ID.

#### [PatientFilter](https://github.com/Coursal/Spark-Examples/tree/main/PatientFilter)
Filtering out patients' records where their `PatientCycleNum` column is equal to `1` and their `Counseling` column is equal to `No`.

#### [ReadFolderFiles](https://github.com/Coursal/Spark-Examples/tree/main/ReadFolderFiles)
Reading a number of files with multiple lines and storing each of them as records in a DataFrame consisting of two columns, `filename` and `content`.

---

_Check out the equivalent **Hadoop Examples** [here](https://github.com/Coursal/Hadoop-Examples)._
