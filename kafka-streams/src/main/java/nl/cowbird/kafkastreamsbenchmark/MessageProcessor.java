package nl.cowbird.kafkastreamsbenchmark;

import nl.cowbird.streamingbenchmarkcommon.Message;

import org.apache.kafka.streams.processor.AbstractProcessor;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by gdibernardo on 09/06/2017.
 */
public class MessageProcessor extends AbstractProcessor<String, String> {

    private ProcessorContext context;

    @Override
    public void process(String key, String value) {
        try {
            JSONObject jsonMessage = new JSONObject(value);
            Message message = new Message(jsonMessage.getString("id"), context.timestamp(), System.currentTimeMillis(), jsonMessage.getDouble("payload"), jsonMessage.getString("reduction_mode"), jsonMessage.getInt("values"));

            this.context.forward(message.getId(), message);
            this.context.commit();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void init(ProcessorContext context) {
        /*  Should I call init? Maybe, boh.... */
        //super.init(context);

        this.context = context;
    }
}
