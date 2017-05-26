package nl.cowbird.sparkbenchmark

/**
  * Created by gdibernardo on 23/05/2017.
  */

import java.util.Properties

import scala.util.Random
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


object ClientProducer extends App
{

    def defaultProperties(): Properties = {
        val properties = new Properties()
        properties.put("serializer.class", "kafka.serializer.StringEncoder")
        properties.put("producer.type", "async")

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
    defaults.put("metadata.broker.list", broker)

    val producerConfigurations = new ProducerConfig(defaults)
    val producer = new Producer[String, String](producerConfigurations)

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
             val message = new KeyedMessage[String, String](topic, payload)
             producer.send(message)
         }

    }

    val time = System.currentTimeMillis()
    var delta = time - currentTime

    System.out.println("Sent " + (numberOfSensors * numberOfEvents * 1000) / (delta) + " messages per second from " + numberOfSensors + " sensors.")



     producer.close()

    /*  Apply reduction for testing.     */
    reductionOperation match {
        case "MEAN" => applyMeanReduction(map)
        case _ => ;
    }
}
