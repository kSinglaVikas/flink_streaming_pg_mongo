# Testing Guide - Flink PostgreSQL to MongoDB CDC Pipeline

## Quick Test

Run the automated test suite:
```bash
./scripts/test-pipeline.sh      # Basic pipeline tests
./scripts/test-embedding.sh     # Order embedding tests
```

## Manual Testing

### 1. Verify Initial Data Sync

**Check PostgreSQL data:**
```bash
docker exec -it postgres-source psql -U postgres -d sourcedb

SELECT * FROM users;
SELECT * FROM orders;
```

**Check MongoDB data:**
```bash
docker exec -it mongodb-target mongosh -u admin -p admin123

use targetdb
db.users.find().pretty()
db.orders.find().pretty()
```

### 2. Test INSERT Operations

**Insert a new user:**
```sql
INSERT INTO users (name, email, age, city) 
VALUES ('Test User', 'test@example.com', 30, 'Seattle');
```

**Verify in MongoDB (wait 2-3 seconds):**
```javascript
db.users.find({email: 'test@example.com'}).pretty()
```

**Insert a new order:**
```sql
INSERT INTO orders (user_id, product_name, quantity, price) 
VALUES (1, 'Tablet', 2, 499.99);
```

**Verify in MongoDB:**
```javascript
db.orders.find({product_name: 'Tablet'}).pretty()
```

### 3. Test UPDATE Operations

**Update user city:**
```sql
UPDATE users SET city = 'Portland', age = 32 WHERE id = 1;
```

**Verify in MongoDB:**
```javascript
db.users.find({_id: 1}).pretty()
```

**Update order quantity:**
```sql
UPDATE orders SET quantity = 5 WHERE id = 1;
```

**Verify in MongoDB:**
```javascript
db.orders.find({_id: 1}).pretty()
```

### 4. Test Bulk Operations

**Bulk insert:**
```sql
INSERT INTO users (name, email, age, city) VALUES 
    ('User A', 'usera@example.com', 25, 'Austin'),
    ('User B', 'userb@example.com', 28, 'Dallas'),
    ('User C', 'userc@example.com', 35, 'Houston');
```

**Verify count:**
```javascript
db.users.countDocuments()
```

### 5. Performance Testing

**Insert 100 records:**
```sql
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO users (name, email, age, city) 
    VALUES (
      'User ' || i, 
      'user' || i || '@test.com', 
      20 + (i % 40), 
      CASE (i % 4)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        ELSE 'Miami'
      END
    );
  END LOOP;
END $$;
```

**Check MongoDB count:**
```javascript
db.users.countDocuments()
```

### 6. Monitor Flink Job

**Flink Web UI:**
- Open: http://localhost:8081
- Check running jobs
- View task metrics
- Monitor checkpoints

**Check Flink logs:**
```bash
docker logs flink-taskmanager -f
docker logs flink-jobmanager -f
```

### 7. Test Data Consistency

**Count records:**
```bash
# PostgreSQL
docker exec postgres-source psql -U postgres -d sourcedb -c "SELECT COUNT(*) FROM users;"

# MongoDB
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.users.countDocuments()"
```

### 8. Test Embedding (Future Enhancement)

The current implementation stores users and orders separately. To test embedding:

**Query user with orders:**
```javascript
// Current: Separate collections
db.users.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "_id",
      foreignField: "user_id",
      as: "orders"
    }
  },
  { $match: { _id: 1 } }
]).pretty()
```

### 9. Test Failure Scenarios

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

### 10. Cleanup Test Data

**Delete test records:**
```sql
-- PostgreSQL
DELETE FROM orders WHERE product_name LIKE '%Test%';
DELETE FROM users WHERE email LIKE '%test%';
```

**MongoDB verification:**
```javascript
db.orders.find({product_name: /Test/}).count()
db.users.find({email: /test/}).count()
```

## Expected Results

| Operation | Latency | Expected Behavior |
|-----------|---------|-------------------|
| INSERT | 1-3s | New document appears in MongoDB |
| UPDATE | 1-3s | Document updated in MongoDB |
| Bulk INSERT (100) | 5-10s | All documents synced |
| Job restart | <30s | Resumes from checkpoint |

## Troubleshooting Tests

**If data not syncing:**
1. Check Flink job status: `docker exec flink-jobmanager flink list`
2. Check logs: `docker logs flink-taskmanager`
3. Verify Postgres CDC: `SELECT * FROM pg_replication_slots;`

**If job fails:**
1. Check error logs: `docker logs flink-taskmanager --tail 100`
2. Verify network: `docker network inspect flink-streaming_flink-network`
3. Test connections: `docker exec flink-jobmanager ping postgres`

## Performance Metrics to Monitor

1. **Records Per Second**: Check in Flink Web UI
2. **Checkpoint Duration**: Should be < 5 seconds
3. **Backpressure**: Monitor in Flink UI
4. **Lag**: Time between Postgres change and MongoDB update

## Success Criteria

✅ Initial snapshot completes within 10 seconds  
✅ CDC events processed within 3 seconds  
✅ No data loss on failure/restart  
✅ Consistent record counts between Postgres and MongoDB  
✅ Job runs without errors for 1+ hour
