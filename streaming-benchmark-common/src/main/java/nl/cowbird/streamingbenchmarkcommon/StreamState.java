package nl.cowbird.streamingbenchmarkcommon;

/**
 * Created by gdibernardo on 06/06/2017.
 */
public class StreamState {

    private String id;

    private Integer counter;

    private Double sum;
    private Double min;
    private Double max;

    private long firstTimpestamp;
    private long lastTimestamp;


    public Boolean isReadyForReduction;


    private String operator;


    public StreamState(String id) {
        this.id = id;

        this.counter = 0;

        this.sum = 0.0;
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;

        this.firstTimpestamp = Long.MAX_VALUE;
        this.lastTimestamp = Long.MIN_VALUE;

        this.isReadyForReduction = false;
    }


    public void appendMessage(Message message) {
        this.sum += message.payload;

        if(message.payload < this.min) {
            this.min = message.payload;
        }

        if(message.payload > this.max) {
            this.max = message.payload;
        }

        if(message.emittedAt < this.firstTimpestamp) {
            this.firstTimpestamp = message.emittedAt;
        }

        if(message.emittedAt > this.lastTimestamp) {
            this.lastTimestamp = message.emittedAt;
        }

        this.operator = message.reduction;

        this.counter++;
    }


    public long firstTimestampInStream() {
        return this.firstTimpestamp;
    }


    public long lastTimestampInStream() {
        return lastTimestamp;
    }


    public String getId() {
        return this.id;
    }


    public Double sum() {
        return this.sum;
    }


    public Double min() {
        return this.min;
    }


    public Double max() {
        return this.max;
    }


    public Integer size() {
        return this.counter;
    }


    public String reductionOperator() {
        return this.operator;
    }
}
