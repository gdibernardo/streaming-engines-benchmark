package nl.cowbird.flinkbenchmark;

/**
 * Created by gdibernardo on 30/05/2017.
 */
public class ResultMessage {

    String id;

    Double resultValue;

    Long processingTime;

    String reduction;


    public ResultMessage(String id, Double resultValue, Long processingTime, String reduction) {
        this.id = id;

        this.resultValue = resultValue;

        this.processingTime = processingTime;

        this.reduction = reduction;
    }

}
