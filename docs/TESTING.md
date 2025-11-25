# Testing Guide - Flink PostgreSQL to MongoDB CDC Pipeline

## Overview

This project uses a simplified validation approach that focuses on verifying the core embedding functionality:

1. **Data Consistency** - User and order counts match between PostgreSQL and MongoDB
2. **Document Embedding** - Orders correctly embedded in user documents
3. **Job Health** - Flink job running without errors

## Quick Validation

Run the validation script to verify embedding correctness:
```bash
./scripts/test-embedding.sh
```

## Load Generation

Generate test data with realistic patterns:
```bash
./scripts/generate-load.sh <users> <orders_per_user>
```

**Examples:**
```bash
./scripts/generate-load.sh 10 4   # Creates 10 users with 4 orders each (40 orders total)
./scripts/generate-load.sh 50 5   # Creates 50 users with 5 orders each (250 orders total)
```

**Important**: The load generator only creates orders for newly created users, ensuring predictable and scoped test data.

## Validation Details

The `test-embedding.sh` script performs three key validations:

### 1. User Count Match
Verifies that the number of users in PostgreSQL equals the number of user documents in MongoDB.

**Query (PostgreSQL):**
```sql
SELECT COUNT(*) FROM users;
```

**Query (MongoDB):**
```javascript
db.users.countDocuments()
```

### 2. Embedded Order Count Match
Verifies that all orders from PostgreSQL are embedded in user documents in MongoDB.

**Query (PostgreSQL):**
```sql
SELECT COUNT(*) FROM orders;
```

**Query (MongoDB):**
```javascript
db.users.aggregate([
  { $unwind: "$orders" },
  { $count: "total" }
])
```

### 3. Flink Job Health
Checks that the Flink CDC job is running without errors.

**Command:**
```bash
docker exec flink-jobmanager flink list
```

## Testing Workflow

### Complete Testing Cycle

1. **Start the pipeline:**
   ```bash
   ./scripts/reset-and-restart.sh
   ```

2. **Generate test data:**
   ```bash
   ./scripts/generate-load.sh 20 3
   ```
   This creates 20 users with 3 orders each (60 orders total).

3. **Wait for CDC processing:**
   Wait 5-10 seconds for the pipeline to process changes.

4. **Validate results:**
   ```bash
   ./scripts/test-embedding.sh
   ```

5. **Expected output:**
   ```
   ✓ User count matches: 20 users in both PostgreSQL and MongoDB
   ✓ Embedded order count matches: 60 orders in both PostgreSQL and MongoDB
   ✓ Flink job is running
   ```

### Incremental Load Testing

You can run load generation multiple times to simulate continuous data:

```bash
# First batch
./scripts/generate-load.sh 10 2

# Wait and validate
./scripts/test-embedding.sh

# Second batch
./scripts/generate-load.sh 15 3

# Validate again
./scripts/test-embedding.sh
```

The validation will show cumulative totals (25 users, 65 orders)

## Manual Testing

### 1. Verify Data Sync

**Check PostgreSQL data:**
```bash
docker exec -it postgres psql -U postgres -d sourcedb

SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM orders;
SELECT * FROM users LIMIT 5;
```

**Check MongoDB data:**
```bash
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin

use targetdb
db.users.countDocuments()
db.users.aggregate([{$unwind: "$orders"}, {$count: "total"}])
db.users.find().limit(1).pretty()
```

### 2. Verify Embedding Structure

**Check that orders are embedded in user documents:**
```javascript
// MongoDB
db.users.findOne({}, {name: 1, email: 1, orders: 1})
```

**Expected structure:**
```javascript
{
  _id: 1,
  name: "John Doe",
  email: "john@example.com",
  orders: [
    {
      order_id: 1,
      product_name: "Laptop",
      quantity: 1,
      price: Decimal128("999.99"),
      order_date: ISODate("2025-11-25T05:03:26.094Z")
    }
  ]
}
```

### 3. Test Real-time CDC

**Insert a new user with orders using load generator:**
```bash
./scripts/generate-load.sh 5 2
```

**Validate immediately:**
```bash
./scripts/test-embedding.sh
```

**Manual verification in PostgreSQL:**
```sql
SELECT u.id, u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name
ORDER BY u.id DESC
LIMIT 5;
```

**Manual verification in MongoDB:**
```javascript
db.users.aggregate([
  { $project: { 
      name: 1, 
      order_count: { $size: { $ifNull: ["$orders", []] } }
    }
  },
  { $sort: { _id: -1 } },
  { $limit: 5 }
])
```

### 4. Performance Testing

**Generate large dataset:**
```bash
./scripts/generate-load.sh 100 5  # 100 users × 5 orders = 500 orders
```

**Monitor processing:**
```bash
watch -n 2 './scripts/test-embedding.sh'
```

**Check processing time:**
- Start time: Note timestamp before load generation
- End time: When validation shows all records synced
- Expected: 10-20 seconds for 500 records

### 5. Monitor Flink Job

**Flink Web UI:**
- Open: http://localhost:8081
- Check running jobs (should see postgres-mongodb-pipeline)
- View task metrics
- Monitor checkpoints (every 5 seconds)
- Check backpressure (should be LOW)

**Check Flink logs:**
```bash
docker logs flink-taskmanager -f
docker logs flink-jobmanager -f
```

**List running jobs:**
```bash
docker exec flink-jobmanager flink list
```

### 6. Test Failure Scenarios

**Stop and restart containers:**
```bash
docker-compose stop postgres
# Wait 10 seconds
docker-compose start postgres
# Check if CDC resumes correctly
```

**Check checkpoint recovery:**
```bash
docker restart flink-taskmanager
# Job should recover from last checkpoint
```

### 7. Complete Reset

**Reset entire pipeline:**
```bash
./scripts/reset-and-restart.sh
```

This will:
- Stop all containers
- Remove volumes (clears all data)
- Restart services
- Reinitialize databases
- Submit Flink job

**Start fresh testing:**
```bash
./scripts/generate-load.sh 10 3
./scripts/test-embedding.sh
```

## Expected Results

| Operation | Latency | Expected Behavior |
|-----------|---------|-------------------|
| Initial snapshot | 5-10s | All existing data synced |
| New user + orders (CDC) | 2-5s | Document appears with embedded orders |
| Bulk load (100 users) | 10-20s | All documents synced |
| Validation check | 1-2s | Counts match between PG and Mongo |
| Job restart | <30s | Resumes from checkpoint |

## Troubleshooting Tests

**If validation fails (counts don't match):**
1. Wait longer (CDC may still be processing): `sleep 10 && ./scripts/test-embedding.sh`
2. Check Flink job status: `docker exec flink-jobmanager flink list`
3. Check logs: `docker logs flink-taskmanager --tail 50`
4. Verify replication slots: `docker exec postgres psql -U postgres -d sourcedb -c "SELECT * FROM pg_replication_slots;"`

**If job not running:**
1. Check error logs: `docker logs flink-taskmanager --tail 100`
2. Check job manager: `docker logs flink-jobmanager --tail 100`
3. Restart pipeline: `./scripts/reset-and-restart.sh`

**If load generation fails:**
1. Check PostgreSQL connection: `docker exec postgres psql -U postgres -d sourcedb -c "SELECT COUNT(*) FROM users;"`
2. Check if tables exist: `docker exec postgres psql -U postgres -d sourcedb -c "\dt"`
3. Reset and try again: `./scripts/reset-and-restart.sh`

**If orders not embedded:**
1. Check user-order relationship: `docker exec postgres psql -U postgres -d sourcedb -c "SELECT u.id, u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id;"`
2. Check MongoDB structure: `docker exec mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "use targetdb" --eval "db.users.findOne()"`
3. Verify KeyedCoProcessFunction is working: Check Flink logs for "Processing user" or "Processing order" messages

## Performance Metrics to Monitor

1. **Records Per Second**: Check in Flink Web UI (http://localhost:8081)
2. **Checkpoint Duration**: Should be < 5 seconds (visible in Flink UI)
3. **Backpressure**: Monitor in Flink UI (should be LOW or OK)
4. **CDC Latency**: Time between PostgreSQL change and MongoDB update (2-5 seconds)
5. **State Size**: Monitor KeyedCoProcessFunction state size (should grow with users)

## Success Criteria

✅ **Data Consistency**: `./scripts/test-embedding.sh` passes all checks  
✅ **Initial snapshot**: Completes within 10 seconds  
✅ **CDC latency**: New records appear in MongoDB within 5 seconds  
✅ **Embedding correctness**: All orders embedded in correct user documents  
✅ **Count accuracy**: User and order counts match between PostgreSQL and MongoDB  
✅ **Job stability**: Runs without errors for 1+ hour  
✅ **Checkpoint health**: Successful checkpoints every 5 seconds  
✅ **No data loss**: Zero records lost on job restart

## Quick Reference

```bash
# Reset and start fresh
./scripts/reset-and-restart.sh

# Generate test data (20 users, 3 orders each)
./scripts/generate-load.sh 20 3

# Validate embedding
./scripts/test-embedding.sh

# Monitor Flink job
open http://localhost:8081

# Check logs
docker logs flink-taskmanager -f
```
