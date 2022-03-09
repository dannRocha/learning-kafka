package store;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    var producer = new KafkaProducer<String, String>(properties());
    var key = "NewOrderConsumerServiceMain";
    var value = "stop";


    var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER_TOPIC", key, value);

    producer.send(record, (content, err) -> {
      if(err != null) {
        err.printStackTrace();
        return;
      }
      System.out.printf("topic: %s | partition: %s | timestamp: %s | content: {%s : %s}", content.topic(), content.partition(), content.hasTimestamp(), content.serializedKeySize(), content.serializedValueSize());
    }).get();

    producer.flush();
    producer.close();

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return properties;
  }
}
