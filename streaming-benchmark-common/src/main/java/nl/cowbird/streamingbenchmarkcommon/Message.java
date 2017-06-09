package nl.cowbird.streamingbenchmarkcommon;

/**
 * Created by gdibernardo on 30/05/2017.
 */
public class Message {

    String id;

    long emittedAt;
    long receivedAt;

    Double payload;

    String reduction;

    Integer values;


    public Message(String id, long emittedAt, long receivedAt, Double payload, String reduction, Integer values) {
        this.id = id;

        this.emittedAt = emittedAt;
        this.receivedAt = receivedAt;

        this.payload = payload;

        this.reduction = reduction;

        this.values = values;
    }


    public static Message initFromMessage(Message message) {
       return new Message(message.id, message.emittedAt, message.receivedAt, message.payload, message.reduction, message.values);
    }


    public void setPayload(Double payload) {
        this.payload = payload;
    }


    public String getId() {
        return this.id;
    }


    public Integer getValues() {
        return this.values;
    }


    public Double getPayload() {
        return this.payload;
    }
}
