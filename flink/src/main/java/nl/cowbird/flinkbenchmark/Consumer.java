package nl.cowbird.flinkbenchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by gdibernardo on 29/05/2017.
 */
public class Consumer {

    public static final String CONSUMER_FLINK_GROUP_ID = "CONSUMER_FLINK_GROUP_ID";

    public static Properties defaultConsumingProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_FLINK_GROUP_ID);

        return properties;
    }
    public static void main(String args[]) throws Exception {
        if(args.length < 3) {
            System.exit(1);
        }

        String broker = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = defaultConsumingProperties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        DataStream<String> messagesStream = environment.addSource(new FlinkKafkaConsumer010<String>(inputTopic, new SimpleStringSchema(), properties));

        messagesStream.rebalance().map(new MapFunction<String, Object>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public Object map(String value) throws Exception {
                    return value;
            }
        }).addSink(new FlinkKafkaProducer010(broker, outputTopic, new SimpleStringSchema()));

        environment.execute();
    }
}
