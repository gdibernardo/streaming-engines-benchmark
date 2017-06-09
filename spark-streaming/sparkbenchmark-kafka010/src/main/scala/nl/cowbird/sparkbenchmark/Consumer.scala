package nl.cowbird.sparkbenchmark


import nl.cowbird.streamingbenchmarkcommon._

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


object Consumer {


  val CACHED_STREAM_URI = "s3n://emr-cluster-spark-bucket/cache_stream.txt"

  val CHECKPOINT_URI = "s3n://emr-cluster-spark-bucket/checkpoint/"

  def updateMessages(key: String,
                     value: Option[Message],
                     state: State[StreamState]): Option[StreamState] = {

    def updateMessagesStream(newMessage: Message): Option[StreamState] = {

      val currentState = state.getOption().getOrElse(new StreamState(newMessage.getId))
      currentState.appendMessage(newMessage)

      var updatedStream: Option[StreamState] = None

      if(currentState.size() >= newMessage.getValues) {
        currentState.isReadyForReduction = true
        updatedStream = Some(currentState)
        state.remove()
      } else {
        state.update(currentState)
      }

      return updatedStream
    }

    value match {
      case Some(newMessage) => updateMessagesStream(newMessage)

      case _ if state.isTimingOut() => state.getOption()
    }
  }


  def applyMeanReduction(stream: StreamState): ResultMessage = {

    val firstIngestionTime = stream.firstTimestampInStream()

    val id = stream.getId

    val resultValue = stream.sum()/stream.size()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return new ResultMessage(id, resultValue, deltaTime, "MEAN")
  }


  def applyMaxReduction(stream: StreamState): ResultMessage = {
    val firstIngestionTime = stream.firstTimestampInStream()
    val id = stream.getId

    val max = stream.max()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return new ResultMessage(id, max, deltaTime, "MAX")
  }


  def applyMinReduction(stream: StreamState): ResultMessage = {
    val firstIngestionTime = stream.firstTimestampInStream()
    val id = stream.getId

    val min = stream.min()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return new ResultMessage(id, min, deltaTime, "MIN")
  }


  def applySumReduction(stream: StreamState): ResultMessage = {
    val firstIngestionTime = stream.firstTimestampInStream()
    val id = stream.getId()

    val sum = stream.sum()

    val currentTime = System.currentTimeMillis()
    val deltaTime = currentTime - firstIngestionTime

    return new ResultMessage(id, sum, deltaTime, "SUM")
  }


  def applyReduction(stream: StreamState): Option[ResultMessage] = {

    if(!stream.isReadyForReduction) {
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

    var joinOperationEnabled = false
    if(args.length == 4) {
      joinOperationEnabled = true
    }

    val Array(broker, inputTopic, outputTopic) = args

    val sparkConf = new SparkConf().setAppName("SparkConsumer")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    streamingContext.checkpoint(CHECKPOINT_URI)

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
      (jsonMessage.getString("id"), new Message(jsonMessage.getString("id"), message.timestamp(), System.currentTimeMillis(), jsonMessage.getDouble("payload"), jsonMessage.getString("reduction_mode"), jsonMessage.getInt("values")))
    })

    var readyForMapMessages = messages

    if(joinOperationEnabled) {
      val cachedStream = streamingContext.textFileStream(CACHED_STREAM_URI)
        .map(line => {
          val jsonMessage = new JSONObject(line)
          (jsonMessage.getString("id"), new Message(jsonMessage.getString("id"), System.currentTimeMillis(), System.currentTimeMillis(), jsonMessage.getDouble("payload"), "-", 10000))
        })

      val joinedStream = messages.join(cachedStream).map(element => {
          val firstTupleMessage = element._2._1
          val message = Message.initFromMessage(firstTupleMessage)
          (element._1, message)
        })

      readyForMapMessages = joinedStream
    }

    val messagesWithState = readyForMapMessages.mapWithState(stateSpec)

    val readyForReductionMessages = messagesWithState.filter(!_.isEmpty).map(_.get).filter(_.isReadyForReduction == true)

    val defaults = ClientProducer.defaultProperties()
    defaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

    readyForReductionMessages.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](defaults)
        partition.foreach(element => {
          val result = applyReduction(element)
          if(!result.isEmpty) {
            val unwrappedResult = result.get

            val jsonPayload = new JSONObject()
            jsonPayload.put("id", unwrappedResult.getId)
            jsonPayload.put("result_value", unwrappedResult.getResultValue)
            jsonPayload.put("processing_time", unwrappedResult.getProcessingTime)

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
