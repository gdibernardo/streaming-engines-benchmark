package nl.cowbird.streamingbenchmark


import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.codehaus.jettison.json.JSONObject

import scala.util.Random

/**
  * Created by gdibernardo on 28/05/2017.
  */

object DataProducer extends App {

  def defaultProperties(): Properties = {
    val properties = new Properties()

    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Int.box(1));

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    return properties
  }


  def applyMeanReduction(map: scala.collection.mutable.Map[Int, Seq[String]]): Unit = {
    map.foreach(element => {
      val count = element._2.length
      val sum = element._2.map(payload => {
        val elements =  payload.split(":")
        elements(2).toDouble
      }).sum

      System.out.println("MEAN for ID " + element._1 + " value: " + sum/count)
    })
  }


  if(args.length < 5) {
    System.exit(1)
  }

  val broker = args(0)
  val topic = args(1)
  val numberOfSensors = args(2).toInt
  // val eventsFrequency = args(3).toInt
  val numberOfEvents = args(3).toInt
  val reductionOperation = args(4)

  val random = new Random()

  val defaults = defaultProperties()
  defaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

  val producer = new KafkaProducer[String, String](defaults)

//  val map = scala.collection.mutable.Map[Int, Seq[String]]()


  while(true) {
    for(id <- Range(0, numberOfSensors)) {
      val currentTime = System.currentTimeMillis()
      for(index <- Range(0, numberOfEvents)) {
        val payload = new JSONObject()

        payload.put("id",  id.toString)
        payload.put("emitted_at", System.currentTimeMillis())
        payload.put("payload", random.nextDouble())
        payload.put("reduction_mode", reductionOperation)
        payload.put("values", numberOfEvents)
        // val payload = id + ":" + System.currentTimeMillis() + ":" + random.nextDouble() + ":" + reductionOperation + ":" + numberOfEvents
//        if(map.contains(id)) {
//          map += (id -> (map.get(id).get :+ payload))
//        }
//        else {
//          map += (id -> Seq(payload))
//        }
        val message = new ProducerRecord[String, String](topic, payload.toString)
        producer.send(message)
      }

      val delta = 1000 - (System.currentTimeMillis() - currentTime)

      if(delta > 0) {
        /*  Wait at most one second.  */
        Thread.sleep(delta)
      }
    }
  }
  producer.close()
}
