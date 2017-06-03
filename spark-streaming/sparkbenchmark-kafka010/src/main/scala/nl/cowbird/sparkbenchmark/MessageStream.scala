package nl.cowbird.sparkbenchmark

/**
  * Created by gdibernardo on 31/05/2017.
  */
class MessageStream extends Serializable {

  private var firstTimestamp: Long = Long.MaxValue

  private var minimumValue: Double = Double.MaxValue
  private var maximumValue: Double = Double.MinValue
  private var sumValues: Double = 0

  private var stream: Seq[Message] = Seq[Message]()

  var readyForReduction: Boolean = false

  def addMessage(message: Message): Unit = {
    if(message.payload > maximumValue) {
      maximumValue = message.payload
    }
    if(message.payload < minimumValue) {
      minimumValue = message.payload
    }

    if(message.emittedAt < firstTimestamp) {
      firstTimestamp = message.emittedAt
    }

    sumValues += message.payload

    stream = stream :+ message
  }


  def streamCount(): Int = {
    return stream.length
  }


  def firstIngestionTime(): Long = {
    return firstTimestamp
  }


  def min(): Double = {
    return minimumValue
  }


  def max(): Double = {
    return maximumValue
  }


  def sum(): Double = {
    return sumValues
  }

  def reductionOperator(): String = {
    return stream(0).reduction
  }


  def messageId(): String = {
    return stream(0).id
  }

  def getStream(): Seq[Message] = {
    return stream
  }
}
