
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StatefulStreamCount {
  def main(args: Array[String]) {
    //Create a conf, running on two local cores
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoint") // We need somewhere to store the state

    // Create a ReceiverInputDStream using the DummySource
    // We could obviously swap this for a socketTextStream or other true receiver
    // However this is much easier to demonstrate
    val stream = ssc.receiverStream(new DummySource(ratePerSec = 1))
    val wordDStream = stream.flatMap(_.split(" ")).map(x => (x, 1))


    /**
      * For mapWithState need to define a function which can combine the state of two DStreams,
      * This is used to update the state of the previous batch with that of the new one.
      *
      * The function must accept the following params:
      * The current Batch Time
      * The key for which the state needs to be updated
      * The value observed at the 'Batch Time' for the key.
      * The current state for the key.
      * Return: the new (key, value) pair where value has the updated state information
      */
    def stateMappingFunc(batchTime: Time, key: String,
                         value: Option[Int],
                         state: State[Long]): Option[(String, Long)] = {
      val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }
    //The state Spec wraps the function and allows us to define properties such as initialState and partitions
    val stateSpec = StateSpec.function(stateMappingFunc _)
      .numPartitions(2)
      .timeout(Seconds(60))


    // This applies the function and emits a merged stream.
    // Since we merge the streams based on keys, this stream will contain the same # of records as the input dstream.
    val wordCountStateStream = wordDStream.mapWithState(stateSpec)

    //Print the first 10 records of the stream
    wordCountStateStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}