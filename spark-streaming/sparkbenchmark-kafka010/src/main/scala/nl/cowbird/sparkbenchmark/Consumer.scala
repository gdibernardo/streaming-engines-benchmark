package nl.cowbird.sparkbenchmark


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.codehaus.jettison.json.JSONObject

/**
  * Created by gdibernardo on 26/05/2017.
  */

case class Message(id: String, emittedAt: Long, receivedAt: Long, payload: Double, reduction: String, values: Int)

case class ResultMessage(id: String, resultValue: Double, processingTime: Long, reduction: String)


object Consumer {

  def updateMessages(key: String,
                     value: Option[Message],
                     state: State[MessageStream]): Option[MessageStream] = {

    def updateMessagesStream(newMessage: Message): Option[MessageStream] = {

      val currentState = state.getOption().getOrElse(new MessageStream())
      currentState.addMessage(newMessage)

      var updatedStream: Option[MessageStream] = None

      if(currentState.streamCount() >= newMessage.values) {
        currentState.readyForReduction = true
        updatedStream = Some(currentState)

        state.remove()
      } else {
        state.update(currentState)
      }

      System.out.println("Updating state ..  " + newMessage.id + " value " + newMessage.payload)
      if(updatedStream != None) {
        System.out.println("Ready for reduction ...  " + updatedStream.get.streamCount())
      }
      return updatedStream
    }

    value match {
      case Some(newMessage) => updateMessagesStream(newMessage)

      case _ if state.isTimingOut() => state.getOption()
    }
  }


  def applyMeanReduction(stream: MessageStream): ResultMessage = {

    val firstIngestionTime = stream.firstIngestionTime()

    val id = stream.messageId()

    val resultValue = stream.sum()/stream.streamCount()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, resultValue, deltaTime, "MEAN")
  }


  def applyMaxReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.firstIngestionTime()
    val id = stream.messageId()

    val max = stream.max()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, max, deltaTime, "MAX")
  }


  def applyMinReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.firstIngestionTime()
    val id = stream.messageId()

    val min = stream.min()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, min, deltaTime, "MIN")
  }


  def applySumReduction(stream: MessageStream): ResultMessage = {
    val firstIngestionTime = stream.firstIngestionTime()
    val id = stream.messageId()

    val sum = stream.sum()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return ResultMessage(id, sum, deltaTime, "SUM")
  }


  def applyReduction(stream: MessageStream): Option[ResultMessage] = {

    if(!stream.readyForReduction) {
      return None
    }

    val operator = stream.reductionOperator()

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
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    streamingContext.checkpoint("s3n://emr-cluster-spark-bucket/checkpoint/")

    val stateSpec = StateSpec.function(updateMessages _).timeout(Minutes(5))

    val kafkaParameters = Map[String, Object](
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
      Subscribe[String, String](Array(inputTopic), kafkaParameters)
    )

    val messages = messageStream.map(message => {
      val jsonMessage = new JSONObject(message.value())
      (jsonMessage.getString("id"), Message(jsonMessage.getString("id"), message.timestamp(), System.currentTimeMillis(), jsonMessage.getDouble("payload"), jsonMessage.getString("reduction_mode"), jsonMessage.getInt("values")))
    })


    val messagesWithState = messages.mapWithState(stateSpec)

    val readyForReductionMessages = messagesWithState.filter(!_.isEmpty).map(_.get).filter(_.readyForReduction == true)


    val defaults = ClientProducer.defaultProperties()
    defaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

    readyForReductionMessages.foreachRDD(rdd => {
      rdd.saveAsTextFile("s3n://emr-cluster-spark-bucket/output/")
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](defaults)
        partition.foreach(element => {
          val result = applyReduction(element)
          if(!result.isEmpty) {
            val unwrappedResult = result.get
            val jsonPayload = new JSONObject()
            jsonPayload.put("id", unwrappedResult.id)
            jsonPayload.put("result_value", unwrappedResult.resultValue)
            jsonPayload.put("processing_time", unwrappedResult.processingTime)
            val message = new ProducerRecord[String, String](outputTopic, jsonPayload.toString)
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
