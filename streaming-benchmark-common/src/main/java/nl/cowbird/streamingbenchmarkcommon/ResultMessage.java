package nl.cowbird.streamingbenchmarkcommon;

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


    public void setId(String id){
        this.id = id;
    }


    public void setProcessingTime(Long processingTime) {
        this.processingTime = processingTime;
    }


    public void setResultValue(Double resultValue) {
        this.resultValue = resultValue;
    }


    public String getId() {
        return this.id;
    }


    public Double getResultValue() {
        return this.resultValue;
    }


    public Long getProcessingTime() {
        return this.processingTime;
    }


    public String getReduction() {
        return this.reduction;
    }
}
