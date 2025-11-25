package com.flink.streaming;

import com.flink.streaming.model.Order;
import com.flink.streaming.model.User;
import com.flink.streaming.sink.MongoDBSink;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Flink DataStream job that reads from PostgreSQL using CDC
 * and writes to MongoDB with embedding support
 */
public class PostgresToMongoJob {

    private static final String POSTGRES_HOST = "postgres";
    private static final int POSTGRES_PORT = 5432;
    private static final String POSTGRES_DATABASE = "sourcedb";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    
    private static final String MONGODB_URI = "mongodb://admin:admin123@mongodb:27017";
    private static final String MONGODB_DATABASE = "targetdb";

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(5000);
        
        // Properties for Debezium to handle decimals as strings
        java.util.Properties debeziumProps = new java.util.Properties();
        debeziumProps.setProperty("decimal.handling.mode", "string");
        
        // Create PostgreSQL CDC source for users table
        SourceFunction<String> usersSource = PostgreSQLSource.<String>builder()
                .hostname(POSTGRES_HOST)
                .port(POSTGRES_PORT)
                .database(POSTGRES_DATABASE)
                .schemaList("public")
                .tableList("public.users")
                .username(POSTGRES_USER)
                .password(POSTGRES_PASSWORD)
                .slotName("flink_users_slot")
                .decodingPluginName("pgoutput")
                .debeziumProperties(debeziumProps)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // Create PostgreSQL CDC source for orders table
        SourceFunction<String> ordersSource = PostgreSQLSource.<String>builder()
                .hostname(POSTGRES_HOST)
                .port(POSTGRES_PORT)
                .database(POSTGRES_DATABASE)
                .schemaList("public")
                .tableList("public.orders")
                .username(POSTGRES_USER)
                .password(POSTGRES_PASSWORD)
                .slotName("flink_orders_slot")
                .decodingPluginName("pgoutput")
                .debeziumProperties(debeziumProps)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // Create data streams
        DataStream<String> usersStream = env
                .addSource(usersSource)
                .name("PostgreSQL Users CDC Source");

        DataStream<String> ordersStream = env
                .addSource(ordersSource)
                .name("PostgreSQL Orders CDC Source");

        // Transform users data
        DataStream<User> transformedUsers = usersStream
                .flatMap(new UserTransformFunction())
                .name("Transform Users");

        // Transform orders data
        DataStream<Order> transformedOrders = ordersStream
                .flatMap(new OrderTransformFunction())
                .name("Transform Orders");

        // Enrich users with their orders using keyBy and connect
        DataStream<User> enrichedUsers = transformedUsers
                .keyBy(User::getId)
                .connect(transformedOrders.keyBy(Order::getUserId))
                .process(new UserOrderEnrichmentFunction())
                .name("Enrich Users with Orders");

        // Sink enriched users to MongoDB (with embedded orders)
        enrichedUsers.addSink(new MongoDBSink<>(
                MONGODB_URI,
                MONGODB_DATABASE,
                "users"
        )).name("MongoDB Users Sink (with embedded orders)");

        // Also sink orders separately for reference (optional)
        transformedOrders.addSink(new MongoDBSink<>(
                MONGODB_URI,
                MONGODB_DATABASE,
                "orders"
        )).name("MongoDB Orders Sink");

        // Execute the job
        env.execute("PostgreSQL to MongoDB CDC Pipeline");
    }
}
