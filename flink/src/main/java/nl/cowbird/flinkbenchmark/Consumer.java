package nl.cowbird.flinkbenchmark;


import nl.cowbird.streamingbenchmarkcommon.*;

import org.apache.flink.api.common.functions.JoinFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.codehaus.jettison.json.JSONObject;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by gdibernardo on 29/05/2017.
 */


public class Consumer {

    public static final String CONSUMER_FLINK_GROUP_ID = "CONSUMER_FLINK_GROUP_ID";

    public static final String CLIENT_ID = "CLIENT_ID";


    public static final String CHECK_POINT_DATA_URI = "s3://emr-cluster-spark-bucket/checkpoint_data_uri/";

    // public static final String CACHED_STREAM_URI = "s3://emr-cluster-spark-bucket/cache_stream.txt";

    /*  TO-DO:  -   Checkpoint interval could be probably increased.    */
    public static final long CHECKPOINT_INTERVAL = 2000;        /*  ms  */

    public static final long WINDOW_SIZE = 1000;                /*  ms  */


    public static Properties defaultConsumingProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_FLINK_GROUP_ID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return properties;
    }


    public static void main(String args[]) throws Exception {

        /*  Not conformed to the Apache Flink's best practices.    */
        /*  ParameterTool should be used instead.   */
        if(args.length < 3) {
            System.exit(1);
        }

        Boolean joinOperationEnabled = false;

        if(args.length == 4)
            joinOperationEnabled = true;

        String broker = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];

        String joiningTopic;

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(CHECKPOINT_INTERVAL);
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        environment.setStateBackend(new FsStateBackend(CHECK_POINT_DATA_URI));

        Properties properties = defaultConsumingProperties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010(inputTopic, new SimpleStringSchema(), properties);
        consumer.assignTimestampsAndWatermarks(new KafkaAssignerAndWatermarks());

        DataStream<String> stream = environment.addSource(consumer);

        DataStream<Tuple2<String,Message>> messagesStream = stream
                .rebalance()
                .map(element -> {
                    JSONObject jsonMessage = new JSONObject(element);
                    return new Tuple2<String, Message>(jsonMessage.getString("id"), new Message(jsonMessage.getString("id"), jsonMessage.getLong("emitted_at"), System.currentTimeMillis(), jsonMessage.getDouble("payload"), jsonMessage.getString("reduction_mode"), jsonMessage.getInt("values")));
                })
                .keyBy(0);

        DataStream<StreamState> readyForReductionStreams;

        if(joinOperationEnabled) {
            joiningTopic = args[3];

            FlinkKafkaConsumer010<String> joinedConsumer = new FlinkKafkaConsumer010<String>(joiningTopic, new SimpleStringSchema(), properties);
            joinedConsumer.assignTimestampsAndWatermarks(new KafkaAssignerAndWatermarks());

            DataStream<Tuple2<String,Message>> joinedStream = environment.addSource(joinedConsumer)
                    .rebalance()
                    .map(element -> {
                        JSONObject jsonMessage = new JSONObject(element);
                        return new Tuple2<String, Message>(jsonMessage.getString("id"), new Message(jsonMessage.getString("id"), jsonMessage.getLong("emitted_at"), System.currentTimeMillis(), jsonMessage.getDouble("payload"), jsonMessage.getString("reduction_mode"), jsonMessage.getInt("values")));
                    }).keyBy(0);

//            DataStream<Tuple2<String,Message>> cachedStream = environment.readTextFile(CACHED_STREAM_URI)
//                    .rebalance()
//                    .map(line -> {
//                        JSONObject jsonMessage = new JSONObject(line);
//                        return new Tuple2<String, Message>(jsonMessage.getString("id"), new Message(jsonMessage.getString("id"), System.currentTimeMillis(), System.currentTimeMillis(), jsonMessage.getDouble("payload"), "-", 10000));
//                    })
//                    .keyBy(0);

            readyForReductionStreams = messagesStream.join(joinedStream)
                    .where(new MessageKeySelector())
                    .equalTo(new MessageKeySelector())
                    .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(WINDOW_SIZE)))
                    .apply(new JoinFunction<Tuple2<String,Message>, Tuple2<String,Message>, Tuple2<String,Message>>() {
                        @Override
                        public Tuple2<String, Message> join(Tuple2<String, Message> firstTuple, Tuple2<String, Message> secondTuple) throws Exception {
                            Message message = Message.initFromMessage(firstTuple.f1);
                            message.setPayload((message.getPayload() + secondTuple.f1.getPayload()) / 2);
                            message.setValues(message.getValues() * secondTuple.f1.getValues());

                            return new Tuple2<>(firstTuple.f0, message);
                        }
                    })
                    .keyBy(0)
                    .map(new StatefulMap());
        } else {
            readyForReductionStreams = messagesStream.map(new StatefulMap());
        }

        readyForReductionStreams.filter(element -> element != null)
                .filter(element -> element.isReadyForReduction)
                .map(new MapReductionOperator())
                .filter(element -> element != null)
                .map(element -> {
                    JSONObject payload = new JSONObject();
                    payload.put("id", element.getId());
                    payload.put("result_value", element.getResultValue());
                    payload.put("processing_time", element.getProcessingTime());

                    return payload.toString();
                })
                .addSink(new FlinkKafkaProducer010(broker, outputTopic, new SimpleStringSchema()));

        environment.execute();
    }


    private static class MessageKeySelector implements KeySelector <Tuple2<String, Message>, String> {
        @Override
        public String getKey(Tuple2<String, Message> value) throws Exception {
            return value.f0;
        }
    }


    private static class KafkaAssignerAndWatermarks implements AssignerWithPeriodicWatermarks<String> {

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            return previousElementTimestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }
    }
}