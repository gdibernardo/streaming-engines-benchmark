package nl.cowbird.sparkbenchmark


import kafka.serializer.StringDecoder
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Milliseconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * Created by gdibernardo on 23/05/2017.
  */


case class Message(id: String, emittedAt: Long, receivedAt: Long, payload: Double, reduction: String, values: Int)

case class MessageStream(stream: Seq[Message], readyForReduction: Boolean)

case class ResultMessage(id: String, resultValue: Double, processingTime: Long, reduction: String)


object Consumer {


  def updateMessages(key: String,
                   value: Option[Message],
                   state: State[MessageStream]): Option[MessageStream] = {

    def updateMessagesStream(newMessage: Message): Option[MessageStream] = {

      val existingMessages = state.getOption().map(_.stream).getOrElse(Seq[Message]())

      val updatedMessages = newMessage +: existingMessages

      var updatedStream: Option[MessageStream] = None

      if(updatedMessages.length >= newMessage.values) {
          state.remove()
          updatedStream = Some(MessageStream(updatedMessages, true))
      } else {
        state.update(MessageStream(updatedMessages, false))
      }

      return updatedStream
    }

    value match {
      case Some(newMessage) => updateMessagesStream(newMessage)
      case _ if state.isTimingOut() => state.getOption()
    }
  }


  def applyMeanReduction(stream: MessageStream): ResultMessage = {
     val firstIngestionTime = stream.stream.min(Ordering.by((message:Message) => message.emittedAt)).emittedAt

     val id = stream.stream(0).id

     val count = stream.stream.length

     val sum = stream.stream.map(_.payload).sum

     val resultValue = sum/count

     val currentTime = System.currentTimeMillis()
     val deltaTime = currentTime - firstIngestionTime

     return ResultMessage(id, resultValue, deltaTime, "MEAN")
  }


  def applyMaxReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.stream.min(Ordering.by((message:Message) => message.emittedAt)).emittedAt
    val id = stream.stream(0).id

    val max = stream.stream.map(_.payload).max

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, max, deltaTime, "MAX")
  }


  def applyMinReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.stream.min(Ordering.by((message:Message) => message.emittedAt)).emittedAt
    val id = stream.stream(0).id

    val min = stream.stream.map(_.payload).min

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, min, deltaTime, "MIN")
  }


  def applySumReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.stream.min(Ordering.by((message:Message) => message.emittedAt)).emittedAt
    val id = stream.stream(0).id

    val sum = stream.stream.map(_.payload).sum

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, sum, deltaTime, "SUM")
  }


  def applyReduction(stream: MessageStream): Option[ResultMessage] = {

    if(!stream.readyForReduction) {
      return None
    }

    val operator = stream.stream(0).reduction

    operator match {

      case "MEAN" => Some(applyMeanReduction(stream))

      case "MAX" => Some(applyMaxReduction(stream))

      case "MIN" => Some(applyMinReduction(stream))

      case "SUM" => Some(applySumReduction(stream))

      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 3) {
      System.exit(1)
    }

    val Array(broker, inputTopic, outputTopic) = args

    val sparkConf = new SparkConf().setAppName("SparkConsumer")
    val streamingContext = new StreamingContext(sparkConf, Milliseconds(750))

    streamingContext.checkpoint("s3n://emr-cluster-spark-bucket/checkpoint/")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)

    /*  Timeout to 5 minutes. */
    val stateSpec = StateSpec.function(updateMessages _).timeout(Minutes(5))

    val messageStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext,
                                                                                                    kafkaParams,
                                                                                                    Set(inputTopic))
    val messages = messageStream.map(message => {
      val elements = message._2.split(":")
      (elements(0), Message(elements(0), elements(1).toLong, System.currentTimeMillis(), elements(2).toDouble, elements(3), elements(4).toInt))
    })

    val messagesWithState = messages.mapWithState(stateSpec)

    val readyForReductionMessages = messagesWithState.filter(!_.isEmpty).map(_.get).filter(_.readyForReduction == true)

    val defaults = ClientProducer.defaultProperties()
    defaults.put("metadata.broker.list", broker)

    readyForReductionMessages.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
       val producerConfigurations = new ProducerConfig(defaults)
       val producer = new Producer[String, String](producerConfigurations)
        partition.foreach(element => {
         val result = applyReduction(element)
         if(!result.isEmpty) {
           val unwrappedResult = result.get
           val payload = "" + unwrappedResult.id + " " + unwrappedResult.resultValue + " time:" + unwrappedResult.processingTime

           val message = new KeyedMessage[String, String](outputTopic, payload)
           producer.send(message)
          }
        })
       producer.close()
      })
    })

    /*  Start the streaming context.  */
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
