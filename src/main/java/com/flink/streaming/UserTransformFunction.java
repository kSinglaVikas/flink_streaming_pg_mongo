package com.flink.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.streaming.model.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform function to parse CDC events and convert to User objects
 */
public class UserTransformFunction implements FlatMapFunction<String, User> {
    private static final Logger LOG = LoggerFactory.getLogger(UserTransformFunction.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<User> out) throws Exception {
        try {
            JsonNode json = objectMapper.readTree(value);
            
            // Extract operation type
            String op = json.has("op") ? json.get("op").asText() : null;
            
            // Skip delete operations (you can handle them if needed)
            if ("d".equals(op)) {
                LOG.info("Skipping delete operation for user");
                return;
            }
            
            // Get the actual data
            JsonNode after = json.has("after") ? json.get("after") : null;
            
            if (after == null) {
                LOG.warn("No 'after' field in CDC event");
                return;
            }
            
            // Create User object
            User user = new User();
            user.setId(after.has("id") ? after.get("id").asInt() : null);
            user.setName(after.has("name") ? after.get("name").asText() : null);
            user.setEmail(after.has("email") ? after.get("email").asText() : null);
            user.setAge(after.has("age") && !after.get("age").isNull() ? after.get("age").asInt() : null);
            user.setCity(after.has("city") && !after.get("city").isNull() ? after.get("city").asText() : null);
            
            // Handle timestamps - can be microseconds (long) or string
            if (after.has("created_at") && !after.get("created_at").isNull()) {
                JsonNode dateNode = after.get("created_at");
                if (dateNode.isNumber()) {
                    long microsSinceEpoch = dateNode.asLong();
                    java.time.Instant instant = java.time.Instant.ofEpochMilli(microsSinceEpoch / 1000);
                    user.setCreatedAt(instant.toString());
                } else {
                    user.setCreatedAt(dateNode.asText());
                }
            }
            
            if (after.has("updated_at") && !after.get("updated_at").isNull()) {
                JsonNode dateNode = after.get("updated_at");
                if (dateNode.isNumber()) {
                    long microsSinceEpoch = dateNode.asLong();
                    java.time.Instant instant = java.time.Instant.ofEpochMilli(microsSinceEpoch / 1000);
                    user.setUpdatedAt(instant.toString());
                } else {
                    user.setUpdatedAt(dateNode.asText());
                }
            }
            
            LOG.info("Transformed user: {}", user);
            out.collect(user);
            
        } catch (Exception e) {
            LOG.error("Error transforming user data: {}", value, e);
        }
    }
}
