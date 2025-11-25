# Flink PostgreSQL to MongoDB CDC Pipeline

Real-time Change Data Capture (CDC) pipeline using Apache Flink to stream data from PostgreSQL to MongoDB with automatic document embedding capabilities.

## 🎯 Features

- ✅ **Real-time CDC**: Captures INSERT, UPDATE operations from PostgreSQL instantly
- ✅ **Document Embedding**: Automatically embeds related data (orders within users)
- ✅ **Exactly-Once Semantics**: Guaranteed data consistency with Flink checkpointing
- ✅ **Type Safety**: Proper handling of decimals, timestamps, and complex types
- ✅ **Docker-Ready**: Complete containerized setup with Docker Compose
- ✅ **Production-Ready**: Includes monitoring, testing, and fault tolerance

## 📋 Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## 🏗️ Architecture

```
PostgreSQL (CDC enabled)
    ↓
Debezium Connector
    ↓
Flink DataStream
    ├─→ User Stream (keyed by user_id)
    └─→ Order Stream (keyed by user_id)
         ↓
    CoProcess Function (Stateful Enrichment)
         ↓
    Enriched User Stream (with embedded orders)
         ↓
    MongoDB Sink
         ↓
MongoDB (Denormalized documents)
```

### Components
- **PostgreSQL 15**: Source database with logical replication
- **Apache Flink 1.18**: Stream processing engine
- **Debezium 1.9**: CDC connector for PostgreSQL
- **MongoDB 7.0**: Target NoSQL database

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Java 11+
- Maven 3.6+

### Setup

1. **Clone and configure environment**
   ```bash
   git clone <repository-url>
   cd Flink-Streaming
   cp .env.example .env
   # Edit .env with your credentials
   ```

2. **Build the application**
   ```bash
   mvn clean package -DskipTests
   ```

3. **Start services**
   ```bash
   docker-compose up -d
   ```

4. **Submit Flink job**
   ```bash
   docker exec flink-jobmanager flink run \
     /opt/flink/usrlib/postgres-mongodb-pipeline-1.0-SNAPSHOT.jar
   ```

5. **Verify it's working**
   ```bash
   # Check a user with embedded orders
   docker exec mongodb-target mongosh -u admin -p admin123 --authenticationDatabase admin \
     --eval "use targetdb" --eval "db.users.findOne({_id: 1})"
   ```

### Accessing Services

| Service | URL/Command | Credentials |
|---------|-------------|-------------|
| Flink Web UI | http://localhost:8081 | None |
| PostgreSQL | `localhost:5432` | See `.env` file |
| MongoDB | `localhost:27017` | See `.env` file |

## 📚 Documentation

- **[Testing Guide](docs/TESTING.md)** - Comprehensive testing instructions
- **[Test Results](docs/TEST_RESULTS.md)** - Latest test results and benchmarks
- **[API Documentation](docs/API.md)** - Code structure and API reference

## 🧪 Testing

Run the embedding test suite:
```bash
./scripts/test-embedding.sh
```

This runs **12 test scenarios** covering:
- Data consistency & initial snapshot
- CDC operations (INSERT, UPDATE)
- Document embedding & referential integrity
- Data type handling (decimals, timestamps)
- Bulk operations & performance
- System health checks

### Load Testing

Generate realistic load to test at scale:
```bash
# Generate 50 users with 5 orders each (default)
./scripts/generate-load.sh

# Custom load: 100 users with 10 orders each
./scripts/generate-load.sh 100 10
```

The load generator creates realistic test data with random names, emails, cities, products, and validates CDC latency and embedding accuracy.

See [docs/TESTING.md](docs/TESTING.md) for detailed test scenarios and expected results.

## ⚙️ Configuration

All credentials are stored in `.env` file (not committed to git):

```bash
# Copy example and edit
cp .env.example .env
```

### Key Configuration Files

| File | Purpose |
|------|---------|
| `.env` | Environment variables & credentials |
| `docker-compose.yml` | Container orchestration |
| `PostgresToMongoJob.java` | Flink job configuration |
| `init-scripts/01-init-db.sql` | Database initialization |

### Environment Variables

See `.env.example` for all available configuration options.

## Project Structure

```
Flink-Streaming/
├── src/main/java/com/flink/streaming/
│   ├── PostgresToMongoJob.java          # Main Flink job
│   ├── UserTransformFunction.java       # User transformation logic
│   ├── OrderTransformFunction.java      # Order transformation logic
│   ├── model/
│   │   ├── User.java                    # User model
│   │   └── Order.java                   # Order model
│   └── sink/
│       └── MongoDBSink.java             # MongoDB sink with embedding support
├── init-scripts/
│   └── 01-init-db.sql                   # PostgreSQL initialization script
├── docker-compose.yml                    # Docker services configuration
├── pom.xml                              # Maven dependencies
└── README.md                            # This file
```

## 📊 Monitoring

### Flink Web UI
Access at http://localhost:8081 to view:
- Running jobs and their status
- Task metrics and throughput
- Checkpoint statistics
- Backpressure monitoring

### Logs
```bash
# Flink logs
docker logs -f flink-taskmanager
docker logs -f flink-jobmanager

# Database logs
docker logs -f postgres-source
docker logs -f mongodb-target
```

## 🔧 Troubleshooting

### Reset and Restart

If CDC stops working or data isn't syncing properly:
```bash
./scripts/reset-and-restart.sh
```

This script will:
1. Cancel running Flink jobs
2. Stop all containers
3. Clean Flink state and checkpoints
4. Clean PostgreSQL replication slots
5. Restart all services with clean state
6. Submit fresh Flink job
7. Verify sync is working

### Common Issues

**Data not syncing?**
```bash
# Check Flink job status
docker exec flink-jobmanager flink list

# Check CDC replication slots
docker exec postgres-source psql -U postgres -d sourcedb -c "SELECT * FROM pg_replication_slots;"

# Check logs for errors
docker logs flink-taskmanager --tail 100
```

**Job keeps restarting?**
```bash
# Restart all services
docker-compose down
docker-compose up -d
```

**MongoDB connection issues?**
Check credentials in `.env` file match your setup.

See [docs/TESTING.md](docs/TESTING.md) for more troubleshooting steps.

## 📂 Example Output

User document with embedded orders:
```json
{
  "_id": 1,
  "name": "John Doe",
  "email": "john@example.com",
  "age": 32,
  "city": "Seattle",
  "orders": [
    {
      "order_id": 1,
      "product_name": "Laptop",
      "quantity": 1,
      "price": Decimal128("999.99"),
      "order_date": "2025-11-25T05:03:26.094Z"
    },
    {
      "order_id": 2,
      "product_name": "Mouse",
      "quantity": 2,
      "price": Decimal128("25.50"),
      "order_date": "2025-11-25T05:03:26.094Z"
    }
  ]
}
```


## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data
docker-compose down -v
```

## 🤝 Contributing

Contributions welcome! Please read the testing guide before submitting PRs.

## 📄 License

MIT License
