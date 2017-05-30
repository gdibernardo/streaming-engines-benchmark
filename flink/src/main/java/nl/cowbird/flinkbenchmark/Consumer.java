package nl.cowbird.flinkbenchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by gdibernardo on 29/05/2017.
 */
public class Consumer {

    public static final String CONSUMER_FLINK_GROUP_ID = "CONSUMER_FLINK_GROUP_ID";

    public static final String CHECK_POINT_DATA_URI = "s3://emr-cluster-spark-bucket/checkpoint_data_uri/";


    public static final long CHECKPOINT_INTERVAL = 5000;


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
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(CHECKPOINT_INTERVAL);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        environment.setStateBackend(new FsStateBackend(CHECK_POINT_DATA_URI));

        Properties properties = defaultConsumingProperties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010(inputTopic, new SimpleStringSchema(), properties);
        // consumer.assignTimestampsAndWatermarks(new KafkaAssignerAndWatermarks());

        DataStream<String> messagesStream = environment.addSource(consumer);

        DataStream<MessageStream> readyForReductionStreams = messagesStream.rebalance().map(element -> {
            String [] elements = element.split(":");
            return new Tuple2<String, Message>(elements[0], new Message(elements[0], Long.parseLong(elements[1]), System.currentTimeMillis(), Double.parseDouble(elements[2]), elements[3], Integer.parseInt(elements[4])));
        }).keyBy(0).map(new StetefulMap()).filter(stream -> stream.isReadyForReduction());

        readyForReductionStreams.map(new MapReductionOperator()).filter(element -> element != null).map(element -> {
            /*  Super ugly. */
            /*  JSON should be used ASAP.    */
            String value = "" + element.id + " " + element.resultValue + " time: " + element.processingTime;
            return value;
        }).addSink(new FlinkKafkaProducer010(broker, outputTopic, new SimpleStringSchema()));


        environment.execute();
    }
}


class KafkaAssignerAndWatermarks implements AssignerWithPeriodicWatermarks<String> {

    private final long maxDelay = 2000;

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        return previousElementTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

}


class StetefulMap extends RichMapFunction<Tuple2 <String, Message>, MessageStream> {

    private transient ValueState<MessageStream> internalState;

    @Override
    public MessageStream map(Tuple2<String, Message> input) throws Exception {

        MessageStream currentState = internalState.value();

        if(currentState == null)
            currentState = new MessageStream();

        currentState.addMessage(input.f1);
        if(input.f1.values == currentState.getStreamCount()) {
            currentState.setReadyForReduction(true);
            internalState.clear();
        } else {
          internalState.update(currentState);
        }

        return currentState;
    }

    @Override
    public void open(Configuration configuration) {
        ValueStateDescriptor<MessageStream> descriptor = new ValueStateDescriptor("stream_state", MessageStream.class);

        internalState = getRuntimeContext().getState(descriptor);
    }
}


class MapReductionOperator implements MapFunction<MessageStream, ResultMessage> {

    private ResultMessage applyMeanReduction(MessageStream stream) {
        Double mean = stream.getSum()/stream.getStreamCount();

        return new ResultMessage(new String(""), mean, new Long(0), "MEAN");
    }


    private ResultMessage applyMaxReduction(MessageStream stream) {
        Double max = stream.getMax();

        return new ResultMessage(new String(""), max, new Long(0), "MAX");
    }


    private ResultMessage applyMinReduction(MessageStream stream) {

        Double min = stream.getMin();

        return new ResultMessage(new String(""), min, new Long(0), "MIN");

    }


    private ResultMessage applySumReduction(MessageStream stream) {

        Double sum = stream.getSum();

        return new ResultMessage(new String(""), sum, new Long(0), "SUM");
    }


    @Override
    public ResultMessage map(MessageStream input) {
        String id = input.firstMessage().id;
        Long ingestionTime = input.firsIngestionTime();
        ResultMessage result = null;
        switch (input.reductionOperator()) {
            case "MEAN": result = applyMeanReduction(input);
                         break;
            case "MAX":  result = applyMaxReduction(input);
                         break;
            case "MIN":  result = applyMinReduction(input);
                         break;
            case "SUM":  result = applySumReduction(input);
                         break;

            default:     break;
        }

        if(result != null) {
            result.id = id;
            result.processingTime = System.currentTimeMillis() - ingestionTime;
        }

        return result;
    }
}