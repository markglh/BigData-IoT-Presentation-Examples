import org.apache.spark.{SparkConf, SparkContext}


object BasicWordCount {
  def main(args: Array[String]): Unit = {

    //Create a conf, running on two local cores
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]") //* will use all cores

    val sc = new SparkContext(conf)

    val input = sc.textFile("words.txt") //load the file

    sc.parallelize(List(1, 2, 3)).reduce(_ + _)
    println(input.count())

    // Split up into words.
    val words = input.flatMap(line => line.split(" "))

    println("Debug String: " + words.toDebugString)

    val counts = words
      .map(word => (word, 1)) //pair each word with 1
      .reduceByKey { case (x, y) => x + y } //combine all matching words

    println("Debug String: " + counts.toDebugString)

    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("counts")


  }
}
