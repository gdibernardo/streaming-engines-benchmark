package nl.cowbird.sparkbenchmark


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by gdibernardo on 26/05/2017.
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
    if (args.length < 3) {
      System.exit(1)
    }

    val Array(broker, inputTopic, outputTopic) = args

    val sparkConf = new SparkConf().setAppName("SparkConsumer")
    val streamingContext = new StreamingContext(sparkConf, Milliseconds(750))

    streamingContext.checkpoint("s3n://emr-cluster-spark-bucket/checkpoint/")

    val stateSpec = StateSpec.function(updateMessages _).timeout(Minutes(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val messageStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array(inputTopic), kafkaParams)
    )

    val messages = messageStream.map(message => {
      val elements = message.value().split(":")
      (elements(0), Message(elements(0), message.timestamp(), System.currentTimeMillis(), elements(2).toDouble, elements(3), elements(4).toInt))
    })
    val messagesWithState = messages.mapWithState(stateSpec)

    val readyForReductionMessages = messagesWithState.filter(!_.isEmpty).map(_.get).filter(_.readyForReduction == true)

    val defaults = ClientProducer.defaultProperties()
    defaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

    readyForReductionMessages.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](defaults)
        partition.foreach(element => {
          val result = applyReduction(element)
          if(!result.isEmpty) {
            val unwrappedResult = result.get
            val payload = "" + unwrappedResult.id + " " + unwrappedResult.resultValue + " time:" + unwrappedResult.processingTime

            val message = new ProducerRecord[String, String](outputTopic, payload)
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
