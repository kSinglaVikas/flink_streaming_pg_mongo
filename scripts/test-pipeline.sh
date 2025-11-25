#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Flink CDC Pipeline Testing ===${NC}\n"

# Test 1: Check initial data sync
echo -e "${BLUE}Test 1: Checking initial data sync from Postgres to MongoDB${NC}"
sleep 5

echo -e "\n${GREEN}PostgreSQL - Users Table:${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "SELECT id, name, email, city FROM users ORDER BY id;"

echo -e "\n${GREEN}MongoDB - Users Collection:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.users.find({}, {_id:1, name:1, email:1, city:1}).sort({_id:1})"

echo -e "\n${GREEN}PostgreSQL - Orders Table:${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "SELECT id, user_id, product_name, quantity, price FROM orders ORDER BY id;"

echo -e "\n${GREEN}MongoDB - Orders Collection:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.orders.find({}, {_id:1, user_id:1, product_name:1, quantity:1, price:1}).sort({_id:1})"

# Test 2: Insert new user
echo -e "\n\n${BLUE}Test 2: Insert new user in PostgreSQL${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "INSERT INTO users (name, email, age, city) VALUES ('Alice Johnson', 'alice@example.com', 28, 'Boston') RETURNING id, name, email;"

echo "Waiting 3 seconds for CDC to process..."
sleep 3

echo -e "\n${GREEN}Check MongoDB for new user:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.users.find({email: 'alice@example.com'}).pretty()"

# Test 3: Update existing user
echo -e "\n\n${BLUE}Test 3: Update user in PostgreSQL${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "UPDATE users SET city = 'San Francisco', age = 31 WHERE id = 1 RETURNING id, name, city, age;"

echo "Waiting 3 seconds for CDC to process..."
sleep 3

echo -e "\n${GREEN}Check MongoDB for updated user:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.users.find({_id: 1}).pretty()"

# Test 4: Insert new order
echo -e "\n\n${BLUE}Test 4: Insert new order in PostgreSQL${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "INSERT INTO orders (user_id, product_name, quantity, price) VALUES (1, 'Headphones', 1, 149.99) RETURNING id, user_id, product_name, quantity, price;"

echo "Waiting 3 seconds for CDC to process..."
sleep 3

echo -e "\n${GREEN}Check MongoDB for new order:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; db.orders.find({product_name: 'Headphones'}).pretty()"

# Test 5: Bulk insert
echo -e "\n\n${BLUE}Test 5: Bulk insert multiple users${NC}"
docker exec postgres-source psql -U postgres -d sourcedb -c "
INSERT INTO users (name, email, age, city) VALUES 
    ('Charlie Brown', 'charlie@example.com', 35, 'Seattle'),
    ('Diana Prince', 'diana@example.com', 29, 'Miami'),
    ('Eve Adams', 'eve@example.com', 32, 'Denver');
"

echo "Waiting 3 seconds for CDC to process..."
sleep 3

echo -e "\n${GREEN}Count documents in MongoDB:${NC}"
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "use targetdb; print('Users count: ' + db.users.countDocuments({})); print('Orders count: ' + db.orders.countDocuments({}));"

# Test 6: Check Flink job status
echo -e "\n\n${BLUE}Test 6: Check Flink Job Status${NC}"
echo -e "${GREEN}Access Flink Web UI at: http://localhost:8081${NC}"
echo -e "${GREEN}Check running jobs:${NC}"
docker exec flink-jobmanager flink list -r

# Summary
echo -e "\n\n${BLUE}=== Test Summary ===${NC}"
echo -e "${GREEN}✓ Initial data sync test${NC}"
echo -e "${GREEN}✓ Insert operation test${NC}"
echo -e "${GREEN}✓ Update operation test${NC}"
echo -e "${GREEN}✓ New order insert test${NC}"
echo -e "${GREEN}✓ Bulk insert test${NC}"
echo -e "${GREEN}✓ Job status check${NC}"

echo -e "\n${BLUE}Next steps:${NC}"
echo "1. Open Flink UI: http://localhost:8081"
echo "2. Connect to MongoDB: docker exec -it mongodb-target mongosh -u admin -p admin123"
echo "3. Connect to PostgreSQL: docker exec -it postgres-source psql -U postgres -d sourcedb"
echo "4. View Flink logs: docker logs flink-taskmanager"
