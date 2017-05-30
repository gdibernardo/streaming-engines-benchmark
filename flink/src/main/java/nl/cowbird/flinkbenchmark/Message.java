package nl.cowbird.flinkbenchmark;

/**
 * Created by gdibernardo on 30/05/2017.
 */
public class Message {
    String id;

    Long emittedAt;
    Long receivedAt;

    Double payload;

    String reduction;

    Integer values;

    public Message(String id, Long emittedAt, Long receivedAt, Double payload, String reduction, Integer values) {
        this.id = id;

        this.emittedAt = emittedAt;
        this.receivedAt = receivedAt;

        this.payload = payload;

        this.reduction = reduction;

        this.values = values;
    }
}
