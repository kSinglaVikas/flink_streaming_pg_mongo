package com.flink.streaming.sink;

import com.flink.streaming.model.Order;
import com.flink.streaming.model.User;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * MongoDB Sink Function that supports embedding documents
 */
public class MongoDBSink<T> extends RichSinkFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSink.class);
    
    private final String mongoUri;
    private final String database;
    private final String collection;
    
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> mongoCollection;

    public MongoDBSink(String mongoUri, String database, String collection) {
        this.mongoUri = mongoUri;
        this.database = database;
        this.collection = collection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        try {
            // Create MongoDB client
            mongoClient = MongoClients.create(mongoUri);
            
            // Get database and collection
            MongoDatabase db = mongoClient.getDatabase(database);
            mongoCollection = db.getCollection(collection);
            
            LOG.info("Connected to MongoDB: {}/{}", database, collection);
        } catch (Exception e) {
            LOG.error("Failed to connect to MongoDB", e);
            throw e;
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        try {
            Document document;
            
            if (value instanceof User) {
                document = convertUserToDocument((User) value);
            } else if (value instanceof Order) {
                document = convertOrderToDocument((Order) value);
            } else {
                LOG.warn("Unsupported type: {}", value.getClass());
                return;
            }
            
            // Upsert document (insert if not exists, update if exists)
            mongoCollection.replaceOne(
                    eq("_id", document.get("_id")),
                    document,
                    new ReplaceOptions().upsert(true)
            );
            
            LOG.info("Successfully written to MongoDB: {}", document.get("_id"));
            
        } catch (Exception e) {
            LOG.error("Error writing to MongoDB", e);
            throw e;
        }
    }

    private Document convertUserToDocument(User user) {
        Document doc = new Document();
        doc.append("_id", user.getId());
        doc.append("name", user.getName());
        doc.append("email", user.getEmail());
        doc.append("age", user.getAge());
        doc.append("city", user.getCity());
        doc.append("created_at", user.getCreatedAt());
        doc.append("updated_at", user.getUpdatedAt());
        
        // Embed orders if available
        if (user.getOrders() != null && !user.getOrders().isEmpty()) {
            List<Document> orderDocs = user.getOrders().stream()
                    .map(order -> {
                        Document orderDoc = new Document();
                        orderDoc.append("order_id", order.getId());
                        orderDoc.append("product_name", order.getProductName());
                        orderDoc.append("quantity", order.getQuantity());
                        orderDoc.append("price", order.getPrice());
                        orderDoc.append("order_date", order.getOrderDate());
                        return orderDoc;
                    })
                    .collect(Collectors.toList());
            doc.append("orders", orderDocs);
        }
        
        return doc;
    }

    private Document convertOrderToDocument(Order order) {
        Document doc = new Document();
        doc.append("_id", order.getId());
        doc.append("user_id", order.getUserId());
        doc.append("product_name", order.getProductName());
        doc.append("quantity", order.getQuantity());
        doc.append("price", order.getPrice());
        doc.append("order_date", order.getOrderDate());
        return doc;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Closed MongoDB connection");
        }
    }
}
