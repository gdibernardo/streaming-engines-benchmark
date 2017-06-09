package nl.cowbird.kafkastreamsbenchmark;

import nl.cowbird.streamingbenchmarkcommon.Message;
import nl.cowbird.streamingbenchmarkcommon.StreamState;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

/**
 * Created by gdibernardo on 09/06/2017.
 */
public class StreamAggregator extends AbstractProcessor<String, Message> {

    private KeyValueStore<String, StreamState> store;

    private ProcessorContext context;

    @Override
    public void process(String id, Message message) {
        StreamState currentState = this.store.get(id);

        if(currentState == null)
            currentState = new StreamState(id);

        currentState.appendMessage(message);
        if(currentState.size() >= message.getValues()) {
            currentState.isReadyForReduction = true;

            this.context.forward(id, currentState);
            this.store.delete(id);
        } else
            this.store.put(id, currentState);

        this.context.commit();
    }


    @Override
    public void punctuate(long timestamp) {
        // This method should be used to emit the current state every timestamp ms.
    }


    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        // this.context.schedule(1000);
        this.store = (KeyValueStore<String, StreamState>) this.context.getStateStore(Consumer.STREAM_STATE);
        Objects.requireNonNull(store,"State store can't be null" );
    }
}
