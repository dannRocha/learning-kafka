package store;

import java.math.BigDecimal;
import java.util.UUID;

public class Order {
  private final String id;
  private final String userId;
  private final BigDecimal value;

  private Order(String id, String userId, BigDecimal value) {
    this.id = id;
    this.userId = userId;
    this.value = value;
  }

  public static Order aOrder(String userId, BigDecimal value) {
    return new Order(UUID.randomUUID().toString(), userId, value);
  }
}
