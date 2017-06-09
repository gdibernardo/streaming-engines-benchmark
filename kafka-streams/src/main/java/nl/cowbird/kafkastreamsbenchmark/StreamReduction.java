package nl.cowbird.kafkastreamsbenchmark;

/**
 * Created by gdibernardo on 09/06/2017.
 */

import nl.cowbird.streamingbenchmarkcommon.StreamState;
import nl.cowbird.streamingbenchmarkcommon.ResultMessage;

import org.apache.kafka.streams.processor.AbstractProcessor;

public class StreamReduction extends AbstractProcessor<String, StreamState> {

    private ResultMessage applyMeanReduction(StreamState stream) {
        Double mean = stream.sum()/stream.size();

        return new ResultMessage(new String(""), mean, new Long(0), "MEAN");
    }


    private ResultMessage applyMaxReduction(StreamState stream) {
        Double max = stream.max();

        return new ResultMessage(new String(""), max, new Long(0), "MAX");
    }


    private ResultMessage applyMinReduction(StreamState stream) {

        Double min = stream.min();

        return new ResultMessage(new String(""), min, new Long(0), "MIN");
    }


    private ResultMessage applySumReduction(StreamState stream) {

        Double sum = stream.sum();

        return new ResultMessage(new String(""), sum, new Long(0), "SUM");
    }

    @Override
    public void process(String key, StreamState streamState) {
        String id = streamState.getId();
        Long ingestionTime = streamState.firstTimestampInStream();
        ResultMessage result = null;
        switch (streamState.reductionOperator()) {
            case "MEAN": result = applyMeanReduction(streamState);
                break;
            case "MAX":  result = applyMaxReduction(streamState);
                break;
            case "MIN":  result = applyMinReduction(streamState);
                break;
            case "SUM":  result = applySumReduction(streamState);
                break;

            default:     break;
        }

        if(result != null) {
            result.setId(id);

            long delta = System.currentTimeMillis() - ingestionTime;
            result.setProcessingTime(delta);

            this.context().forward(key, result);
            this.context().commit();
        }
    }
}