package com.flink.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.streaming.model.Order;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Transform function to parse CDC events and convert to Order objects
 */
public class OrderTransformFunction implements FlatMapFunction<String, Order> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderTransformFunction.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<Order> out) throws Exception {
        try {
            JsonNode json = objectMapper.readTree(value);
            
            // Extract operation type
            String op = json.has("op") ? json.get("op").asText() : null;
            
            // Skip delete operations
            if ("d".equals(op)) {
                LOG.info("Skipping delete operation for order");
                return;
            }
            
            // Get the actual data
            JsonNode after = json.has("after") ? json.get("after") : null;
            
            if (after == null) {
                LOG.warn("No 'after' field in CDC event");
                return;
            }
            
            // Create Order object
            Order order = new Order();
            order.setId(after.has("id") ? after.get("id").asInt() : null);
            order.setUserId(after.has("user_id") && !after.get("user_id").isNull() ? after.get("user_id").asInt() : null);
            order.setProductName(after.has("product_name") && !after.get("product_name").isNull() ? after.get("product_name").asText() : null);
            order.setQuantity(after.has("quantity") && !after.get("quantity").isNull() ? after.get("quantity").asInt() : null);
            
            // Handle price - can be a number or string
            if (after.has("price") && !after.get("price").isNull()) {
                JsonNode priceNode = after.get("price");
                if (priceNode.isNumber()) {
                    order.setPrice(priceNode.decimalValue());
                } else if (priceNode.isTextual()) {
                    try {
                        order.setPrice(new BigDecimal(priceNode.asText()));
                    } catch (NumberFormatException e) {
                        LOG.warn("Could not parse price: {}", priceNode.asText());
                    }
                }
            }
            
            // Handle order_date - can be timestamp (long) or string
            if (after.has("order_date") && !after.get("order_date").isNull()) {
                JsonNode dateNode = after.get("order_date");
                if (dateNode.isNumber()) {
                    // Convert microseconds timestamp to ISO string
                    long microsSinceEpoch = dateNode.asLong();
                    java.time.Instant instant = java.time.Instant.ofEpochMilli(microsSinceEpoch / 1000);
                    order.setOrderDate(instant.toString());
                } else {
                    order.setOrderDate(dateNode.asText());
                }
            }
            
            LOG.info("Transformed order: {}", order);
            out.collect(order);
            
        } catch (Exception e) {
            LOG.error("Error transforming order data: {}", value, e);
        }
    }
}
