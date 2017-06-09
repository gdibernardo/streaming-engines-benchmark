package nl.cowbird.kafkastreamsbenchmark;

import nl.cowbird.streamingbenchmarkcommon.ResultMessage;

import org.apache.kafka.streams.processor.AbstractProcessor;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by gdibernardo on 09/06/2017.
 */
public class ResultProcessor extends AbstractProcessor<String, ResultMessage> {
    @Override
    public void process(String key, ResultMessage resultMessage) {
        try {

            JSONObject payload = new JSONObject();
            payload.put("id", resultMessage.getId());
            payload.put("result_value", resultMessage.getResultValue());
            payload.put("processing_time", resultMessage.getProcessingTime());

            this.context().forward(key, payload.toString());
            this.context().commit();

        } catch (JSONException exception) {
            exception.printStackTrace();
            System.exit(1);
        }
    }
}
