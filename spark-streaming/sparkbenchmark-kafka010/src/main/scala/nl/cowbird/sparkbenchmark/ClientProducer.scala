package nl.cowbird.sparkbenchmark


import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig



/**
  * Created by gdibernardo on 26/05/2017.
  */

/*  Old Scala Kafka data producer.  */
/*  It should be removed.         */

object ClientProducer extends App {

  def defaultProperties(): Properties = {
    val properties = new Properties()

    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Int.box(1));

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    return properties
  }
}
