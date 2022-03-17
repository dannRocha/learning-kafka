package store;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    try(var dispatcher = new KafkaDispatcher<Order>()) {
      var key = "NewOrderConsumerServiceMain";
      var value = "stop";
      var userId = UUID.randomUUID().toString();
      var amount =  new BigDecimal(Math.random() * 100 + 1);
      var order = Order.aOrder(userId,  amount);
      dispatcher.send("ECOMMERCE_NEW_ORDER_TOPIC", key, order);
    }
  }


}
