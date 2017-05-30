package nl.cowbird.flinkbenchmark;

import java.util.ArrayList;

/**
 * Created by gdibernardo on 30/05/2017.
 */
public class MessageStream {

    private Boolean readyForReduction;

    private ArrayList<Message> stream;

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
        stream.add(message);
    }


    public Message firstMessage() {
        return stream.get(0);
    }

    public Message lastMessage() {
        return stream.get(stream.size() - 1);
    }

    public String reductionOperator() {
        return stream.get(0).reduction;
    }

    public Long firsIngestionTime() {
        Long minimum = stream.get(0).emittedAt;

        for(Message message : stream) {
            if(message.emittedAt < minimum) {
                minimum = message.emittedAt;
            }
        }

        return minimum;
    }

    public Double getMin() {
        Double minimum = stream.get(0).payload;

        for(Message message : stream) {
            if(message.payload < minimum) {
                minimum = message.payload;
            }
        }

        return minimum;
    }

    public Double getMax() {
        Double maximum = stream.get(0).payload;

        for(Message message : stream) {
            if(message.payload > maximum) {
                maximum = message.payload;
            }
        }

        return maximum;
    }

    public Double getSum() {
        Double sum = 0.0;

        for(Message message : stream) {
            sum += message.payload;
        }

        return sum;
    }
}
