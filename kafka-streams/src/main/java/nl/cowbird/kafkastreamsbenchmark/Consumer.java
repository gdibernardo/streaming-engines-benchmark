package nl.cowbird.kafkastreamsbenchmark;

/**
 * Created by gdibernardo on 08/06/2017.
 */

import nl.cowbird.streamingbenchmarkcommon.StreamState;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import serializer.JSONDeserializer;
import serializer.JSONSerializer;


import java.util.Properties;

public class Consumer {

    public final static String APPLICATION_ID = "CONSUMER_APP_ID";
    public final static String CLIENT_ID = "CLIENT_ID";


    private final static String SOURCE = "SOURCE";
    private final static String SINK = "SINK";

    private final static String MESSAGE_PROCESSOR = "TO_MESSAGE_PROCESSOR";
    private final static String STREAM_AGGREGATOR_PROCESSOR = "STREAM_AGGREGATOR_PROCESSOR";
    private final static String STREAM_APPLY_REDUCTION_PROCESSOR = "STREAM_APPLY_REDUCTION_PROCESSOR";
    private final static String RESULT_PROCESSOR = "RESULT_PROCESSOR";

    public final static String STREAM_STATE = "STREAM_STATE";


    public static Properties defaultConsumingProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ConsumerTimestampExtractor.class);

        return properties;
    }


    public static void main(String args[])  {
        if(args.length < 3) {
            System.exit(1);
        }

        Boolean joinOperationEnabled = false;

        if(args.length == 4)
            joinOperationEnabled = true;

        String broker = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];

        Properties properties = defaultConsumingProperties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        StreamsConfig streamingConfig = new StreamsConfig(properties);

        JSONDeserializer<StreamState> streamStateDeserializer = new JSONDeserializer<>(StreamState.class);
        JSONSerializer<StreamState> streamStateSerializer = new JSONSerializer<>();

        Serde<StreamState> streamStateSerde = Serdes.serdeFrom(streamStateSerializer, streamStateDeserializer);
        TopologyBuilder builder = new TopologyBuilder();
        if(joinOperationEnabled) {
            /*  Join operations can be performed between KStreams (and/or KTables). */
            /*  We should initialize a KStreamBuilder instead of a TopologyBuilder. */
            /*  TO-DO:  -   Some stuff. */
        }
        else {
            builder .addSource(SOURCE, new StringDeserializer(), new StringDeserializer(), inputTopic)
                    .addProcessor(MESSAGE_PROCESSOR, MessageProcessor::new, SOURCE)
                    .addProcessor(STREAM_AGGREGATOR_PROCESSOR, StreamAggregator::new, MESSAGE_PROCESSOR)
                    .addStateStore(Stores.create(STREAM_STATE).withStringKeys().withValues(streamStateSerde).inMemory().build(), STREAM_AGGREGATOR_PROCESSOR)
                    .addProcessor(STREAM_APPLY_REDUCTION_PROCESSOR, StreamReduction::new, STREAM_AGGREGATOR_PROCESSOR)
                    .addProcessor(RESULT_PROCESSOR, ResultProcessor::new, STREAM_APPLY_REDUCTION_PROCESSOR)
                    .addSink(SINK, outputTopic, new StringSerializer(), new StringSerializer(), RESULT_PROCESSOR);
        }

        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamingConfig);
        kafkaStreams.start();
    }

}
