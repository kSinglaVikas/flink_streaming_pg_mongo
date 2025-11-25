# API Documentation

## Core Classes

### PostgresToMongoJob
Main entry point for the Flink streaming job.

**Key Methods:**
- `main(String[] args)`: Initializes environment, creates sources, and starts the pipeline

**Configuration:**
- `POSTGRES_HOST`: PostgreSQL hostname (default: "postgres")
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `MONGODB_URI`: MongoDB connection string
- `CHECKPOINT_INTERVAL`: Time between checkpoints (default: 5000ms)

### UserOrderEnrichmentFunction
Stateful CoProcessFunction that enriches users with their orders.

**Type Parameters:**
- Input 1: `User` - User stream
- Input 2: `Order` - Order stream  
- Output: `User` - Enriched user with embedded orders

**State:**
- `MapState<Integer, Order>` - Stores orders by order_id
- `MapState<Integer, User>` - Stores users by user_id

**Methods:**
- `processElement1(User user, Context ctx, Collector<User> out)`: Processes user updates
- `processElement2(Order order, Context ctx, Collector<User> out)`: Processes order updates

### Transform Functions

#### UserTransformFunction
`FlatMapFunction<String, User>`

Transforms Debezium CDC JSON events into User POJOs.

**Input:** CDC JSON string from PostgreSQL
**Output:** User object

**Handles:**
- INSERT operations (op: "c" or "r")
- UPDATE operations (op: "u")
- Skips DELETE operations (op: "d")
- Timestamp conversion from microseconds to ISO-8601
- Null value handling

#### OrderTransformFunction
`FlatMapFunction<String, Order>`

Transforms Debezium CDC JSON events into Order POJOs.

**Input:** CDC JSON string from PostgreSQL
**Output:** Order object

**Special Handling:**
- Decimal prices (NUMERIC type) converted via string
- Timestamp conversion
- Null-safe field extraction

### Data Models

#### User
```java
public class User implements Serializable {
    private Integer id;
    private String name;
    private String email;
    private Integer age;
    private String city;
    private String createdAt;
    private String updatedAt;
    private List<Order> orders;
}
```

#### Order
```java
public class Order implements Serializable {
    private Integer id;
    private Integer userId;
    private String productName;
    private Integer quantity;
    private BigDecimal price;
    private String orderDate;
}
```

### MongoDBSink
`RichSinkFunction<T>`

Custom sink for writing to MongoDB with embedding support.

**Configuration:**
- `mongoUri`: MongoDB connection string
- `database`: Target database name
- `collection`: Target collection name

**Methods:**
- `open(Configuration parameters)`: Initializes MongoDB connection
- `invoke(T value, Context context)`: Writes documents to MongoDB
- `close()`: Closes MongoDB connection

**Features:**
- Upsert operations (insert or update)
- Automatic document conversion
- Support for embedded arrays
- Connection pooling

## CDC Event Format

### Debezium JSON Structure
```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "city": "New York"
  },
  "source": {
    "version": "1.9.7.Final",
    "connector": "postgresql",
    "name": "postgres_cdc_source",
    "ts_ms": 1632847065713,
    "snapshot": "false",
    "db": "sourcedb",
    "schema": "public",
    "table": "users"
  },
  "op": "c",
  "ts_ms": 1632847065753
}
```

### Operation Types
- `"c"` or `"r"` - INSERT/CREATE
- `"u"` - UPDATE
- `"d"` - DELETE (skipped by default)

## Stream Processing Flow

```
Source (CDC)
    ↓
Transform Function (FlatMap)
    ↓
KeyBy (user_id)
    ↓
Connect Streams
    ↓
CoProcess (Enrichment)
    ↓
Sink (MongoDB)
```

## State Management

### State Lifecycle
1. **Initialization**: State created in `open()` method
2. **Updates**: State updated on each element
3. **Checkpoints**: State saved at configured intervals
4. **Recovery**: State restored from checkpoint on failure

### State Backend
- Type: Filesystem
- Checkpoint Directory: `/tmp/flink-checkpoints`
- Savepoint Directory: `/tmp/flink-savepoints`

## Error Handling

### Retry Logic
- CDC connector: Automatic retry with exponential backoff
- MongoDB writes: Manual retry on connection errors

### Logging
- User transformations: `UserTransformFunction` logger
- Order transformations: `OrderTransformFunction` logger
- MongoDB operations: `MongoDBSink` logger

## Performance Tuning

### Parallelism
Set in docker-compose.yml:
```yaml
taskmanager.numberOfTaskSlots: 2
```

### Checkpointing
Configure in PostgresToMongoJob:
```java
env.enableCheckpointing(5000); // 5 seconds
```

### MongoDB Batch Size
Currently single document writes. For batching, modify `MongoDBSink.invoke()` to buffer documents.

## Extension Points

### Adding New Tables
1. Create CDC source in `PostgresToMongoJob`
2. Create transform function
3. Create/update data model
4. Add sink or enrichment logic

### Custom Enrichment
Extend `KeyedCoProcessFunction` for custom join logic.

### Additional Sinks
Implement `SinkFunction<T>` for other targets (Elasticsearch, Kafka, etc.).
