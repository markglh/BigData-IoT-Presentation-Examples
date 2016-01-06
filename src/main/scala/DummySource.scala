import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver._

//Provides a continuous stream of words
//Originally stolen from here:
//https://docs.cloud.databricks.com/docs/spark/1.6/index.html#examples/Streaming%20mapWithState.html
class DummySource(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  val words = """Induced by these considerations, and influenced by the authority of
    |Orgetorix, they determined to provide such things as were necessary
    |for their expedition - to buy up as great a number as possible of
    |beasts of burden and wagons - to make their sowings as large as possible,
    |so that on their march plenty of corn might be in store - and to establish
    |peace and friendship with the neighboring states. They reckoned that
    |a term of two years would be sufficient for them to execute their
    |designs; they fix by decree their departure for the third year. Orgetorix
    |is chosen to complete these arrangements. He took upon himself the
    |office of embassador to the states: on this journey he persuades Casticus,
    |the son of Catamantaledes (one of the Sequani, whose father had possessed
    |the sovereignty among the people for many years, and had been styled
    |"friend" by the senate of the Roman people), to seize upon the sovereignty
    |in his own state, which his father had held before him, and he likewise
    |persuades Dumnorix, an Aeduan, the brother of Divitiacus, who at that
    |time possessed the chief authority in the state, and was exceedingly
    |beloved by the people, to attempt the same, and gives him his daughter
    |in marriage. He proves to them that to accomplish their attempts was
    |a thing very easy to be done, because he himself would obtain the
    |government of his own state; that there was no doubt that the Helvetii
    |were the most powerful of the whole of Gaul; he assures them that
    |he will, with his own forces and his own army, acquire the sovereignty
    |for them. Incited by this speech, they give a pledge and oath to one
    |another, and hope that, when they have seized the sovereignty, they
    |will, by means of the three most powerful and valiant nations, be
    |enabled to obtain possession of the whole of Gaul.""".stripMargin

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {
      store(words)
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}