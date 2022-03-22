package store;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

  KafkaProducer<String, T> producer;
  public KafkaDispatcher() {
    producer = new KafkaProducer<>(properties());
  }

  public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {

    var record = new ProducerRecord<>(topic, key, value);

    producer.send(record, (content, err) -> {
      if(err != null) {
        err.printStackTrace();
        return;
      }
      System.out.printf("topic: %s | partition: %s | timestamp: %s | content: {%s : %s}", content.topic(), content.partition(), content.hasTimestamp(), content.serializedKeySize(), content.serializedValueSize());
    }).get();
  }

  private Properties properties() {
    var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 0, 1, all
    return properties;
  }

  @Override
  public void close(){
    producer.close();
  }
}
