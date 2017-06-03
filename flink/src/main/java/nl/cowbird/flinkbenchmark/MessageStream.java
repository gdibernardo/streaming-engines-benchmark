package nl.cowbird.flinkbenchmark;

import java.util.ArrayList;

/**
 * Created by gdibernardo on 30/05/2017.
 */

public class MessageStream {

    private Boolean readyForReduction;


    private ArrayList<Message> stream;


    private Double minimumElementInStream = Double.MAX_VALUE;

    private Double maximumElementInStream = Double.MIN_VALUE;

    private Double sum = 0.0;


    private Long firstTimestamp = Long.MAX_VALUE;


    public MessageStream() {
        stream = new ArrayList<>();
        readyForReduction = false;
    }


    public int getStreamCount() {
        return stream.size();
    }


    public Boolean isReadyForReduction() {
        return readyForReduction;
    }


    public void setReadyForReduction(Boolean value) {
        readyForReduction = value;
    }


    public void addMessage(Message message) {

        if(message.payload > maximumElementInStream)
            maximumElementInStream = message.payload;

        if(message.payload < minimumElementInStream)
            minimumElementInStream = message.payload;

        if(message.emittedAt < firstTimestamp)
            firstTimestamp = message.emittedAt;

        sum += message.payload;

        stream.add(message);
    }


    public String messageId() {
        return stream.get(0).id;
    }


    public String reductionOperator() {
        return stream.get(0).reduction;
    }


    public Long firsIngestionTime() {
        return firstTimestamp;
    }


    public Double getMin() {
        return minimumElementInStream;
    }


    public Double getMax() {
        return maximumElementInStream;
    }


    public Double getSum() {
        return sum;
    }
}
