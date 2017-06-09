package nl.cowbird.flinkbenchmark;

/**
 * Created by gdibernardo on 09/06/2017.
 */

import nl.cowbird.streamingbenchmarkcommon.*;

import org.apache.flink.api.common.functions.MapFunction;


public class MapReductionOperator implements MapFunction<StreamState, ResultMessage> {

    private ResultMessage applyMeanReduction(StreamState stream) {
        Double mean = stream.sum() / stream.size();

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
    public ResultMessage map(StreamState input) {
        String id = input.getId();
        Long ingestionTime = input.firstTimestampInStream();
        ResultMessage result = null;
        switch (input.reductionOperator()) {
            case "MEAN":
                result = applyMeanReduction(input);
                break;
            case "MAX":
                result = applyMaxReduction(input);
                break;
            case "MIN":
                result = applyMinReduction(input);
                break;
            case "SUM":
                result = applySumReduction(input);
                break;

            default:
                break;
        }

        if (result != null) {
            result.setId(id);
            result.setProcessingTime(System.currentTimeMillis() - ingestionTime);
        }

        return result;
    }
}