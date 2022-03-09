package store;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

public class NewOrderConsumerServiceMain {

  public static void main(String[] args) {
    var consumer = new KafkaConsumer<String, String>(properties());
    consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER_TOPIC"));
    final Duration delay = Duration.ofMillis(100);


    Predicate<ConsumerRecord<String, String>> stopService =
        (record) -> record.key().equalsIgnoreCase(NewOrderConsumerServiceMain.class.getSimpleName())
            && record.value().equalsIgnoreCase("stop");
    var running = true;
    while (running) {
      try { Thread.sleep(Duration.ofSeconds(1).toMillis()); }
      catch (Exception e) {e.printStackTrace(); }
      
      var records = consumer.poll(delay);

      if(records.isEmpty()) {
        System.out.println(String.format("%s - without service", new Date()));
        continue;
      }

      for(var record: records) {
        System.out.println(String.format("%s processing - {%s: %s}", new Date(), record.key(), record.value()));
        if(stopService.test(record))
          running = false;
          break;
      }
    }

    consumer.close();
  }

  private static Properties properties() {
    var properties = new Properties();

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, NewOrderConsumerServiceMain.class.getSimpleName());

    return properties;
  }
}
