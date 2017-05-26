package nl.cowbird.sparkbenchmark


import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random


/**
  * Created by gdibernardo on 26/05/2017.
  */

object ClientProducer extends App {

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


  val broker = args(0)
  val topic = args(1)
  val numberOfSensors = args(2).toInt
  val numberOfEvents = args(3).toInt
  val reductionOperation = args(4)

  val random = new Random()

  val defaults = defaultProperties()

  defaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

  val producer = new KafkaProducer[String, String](defaults)

  val map = scala.collection.mutable.Map[Int, Seq[String]]()
  val currentTime = System.currentTimeMillis()

  for(index <- Range(0, numberOfEvents))
  {
    /* We should consider JSON formatting. */
    for(id <- Range(0, numberOfSensors)) {
      val payload = id + ":" + System.currentTimeMillis() + ":" + random.nextDouble() + ":" + reductionOperation + ":" + numberOfEvents
      if(map.contains(id)) {
        map += (id -> (map.get(id).get :+ payload))
      }
      else {
        map += (id -> Seq(payload))
      }

      val message = new ProducerRecord[String, String](topic, payload)
      producer.send(message)
    }
  }

  val time = System.currentTimeMillis()
  val delta = time - currentTime

  System.out.println("Sent " + (numberOfSensors * numberOfEvents * 1000) / (delta) + " messages per second from " + numberOfSensors + " sensors.")

  producer.close()

  /*  Apply reduction for testing.     */
  reductionOperation match {
    case "MEAN" => applyMeanReduction(map)
    case _ => ;
  }
}
