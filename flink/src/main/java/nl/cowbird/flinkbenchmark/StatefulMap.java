package nl.cowbird.flinkbenchmark;

/**
 * Created by gdibernardo on 09/06/2017.
 */

import nl.cowbird.streamingbenchmarkcommon.Message;
import nl.cowbird.streamingbenchmarkcommon.StreamState;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.configuration.Configuration;


public class StatefulMap extends RichMapFunction<Tuple2<String, Message>, StreamState> {

    private transient ValueState<StreamState> internalState;

    static final String STATE_DESCRIPTOR_NAME = "STATE_DESCRIPTOR_NAME";


    @Override
    public StreamState map(Tuple2<String, Message> input) throws Exception {

        StreamState currentState = internalState.value();

        if(currentState == null)
            currentState = new StreamState(input.f1.getId());

        currentState.appendMessage(input.f1);

        StreamState state = null;

        if(currentState.size() >= input.f1.getValues()) {
            currentState.isReadyForReduction = true;
            state = currentState;
            internalState.clear();
        } else {
            internalState.update(currentState);
        }

        return state;
    }


    @Override
    public void open(Configuration configuration) {
        ValueStateDescriptor<StreamState> descriptor = new ValueStateDescriptor(STATE_DESCRIPTOR_NAME, StreamState.class);

        internalState = getRuntimeContext().getState(descriptor);
    }
}
