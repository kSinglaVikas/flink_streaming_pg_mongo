#!/bin/bash

# Embedding Validation for Flink CDC Pipeline
# Validates that data from PostgreSQL is correctly synced and embedded in MongoDB

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Helper functions
get_pg_count() {
    docker exec postgres-source psql -U postgres -d sourcedb -t -c "$1" | xargs
}

get_mongo_count() {
    docker exec mongodb-target mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db = db.getSiblingDB('targetdb'); $1"
}

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   FLINK CDC EMBEDDING VALIDATION      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# Check if there's data to validate
PG_USERS=$(get_pg_count "SELECT COUNT(*) FROM users;")
PG_ORDERS=$(get_pg_count "SELECT COUNT(*) FROM orders;")

if [ "$PG_USERS" -eq 0 ] || [ "$PG_ORDERS" -eq 0 ]; then
    echo -e "${YELLOW}⚠ No data found in PostgreSQL${NC}\n"
    echo -e "Please generate test data first:"
    echo -e "  ${BLUE}./scripts/generate-load.sh 50 5${NC}  # 50 users, 5 orders each\n"
    exit 0
fi

echo -e "${YELLOW}Source Data (PostgreSQL):${NC}"
echo -e "  Users:  $PG_USERS"
echo -e "  Orders: $PG_ORDERS\n"

# Get MongoDB counts
MONGO_USERS=$(get_mongo_count "db.users.countDocuments()")
TOTAL_EMBEDDED=$(get_mongo_count 'db.users.aggregate([{$project: {orderCount: {$cond: {if: {$isArray: "$orders"}, then: {$size: "$orders"}, else: 0}}}}, {$group: {_id: null, total: {$sum: "$orderCount"}}}]).toArray()[0]?.total || 0')

echo -e "${YELLOW}Target Data (MongoDB):${NC}"
echo -e "  Users:          $MONGO_USERS"
echo -e "  Embedded Orders: $TOTAL_EMBEDDED\n"

# Validation
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}VALIDATION RESULTS${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"

PASSED=0
FAILED=0

# Check user count
if [ "$PG_USERS" -eq "$MONGO_USERS" ]; then
    echo -e "${GREEN}✓ User count matches${NC} ($PG_USERS)"
    ((PASSED++))
else
    echo -e "${RED}✗ User count mismatch${NC} (PG: $PG_USERS, Mongo: $MONGO_USERS)"
    ((FAILED++))
fi

# Check order embedding
if [ "$PG_ORDERS" -eq "$TOTAL_EMBEDDED" ]; then
    echo -e "${GREEN}✓ All orders embedded correctly${NC} ($PG_ORDERS)"
    ((PASSED++))
else
    echo -e "${RED}✗ Order embedding mismatch${NC} (PG: $PG_ORDERS, Embedded: $TOTAL_EMBEDDED)"
    ((FAILED++))
fi

# Check Flink job status
JOB_STATUS=$(docker exec flink-jobmanager flink list -r 2>/dev/null | grep RUNNING || echo "")
if [[ "$JOB_STATUS" == *"RUNNING"* ]]; then
    echo -e "${GREEN}✓ Flink job is running${NC}"
    ((PASSED++))
else
    echo -e "${RED}✗ Flink job is not running${NC}"
    ((FAILED++))
fi

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}SUMMARY${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}\n"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All validations passed!${NC}\n"
    exit 0
else
    echo -e "${RED}✗ Some validations failed${NC}\n"
    exit 1
fi
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

PASSED=0
FAILED=0

# Helper functions
log_test() {
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}TEST $1: $2${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
    ((PASSED++))
}

log_error() {
    echo -e "${RED}✗ $1${NC}"
    ((FAILED++))
}

wait_for_cdc() {
    sleep $1
}

get_pg_count() {
    docker exec postgres-source psql -U postgres -d sourcedb -t -c "$1" | xargs
}

get_mongo_count() {
    docker exec mongodb-target mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db = db.getSiblingDB('targetdb'); $1"
}

# Start tests
echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║    FLINK CDC EMBEDDING TEST SUITE     ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# ============================================================================
# TEST 1: Data Consistency - Initial Snapshot
# ============================================================================
log_test "1" "Initial Snapshot Data Consistency"

PG_USERS=$(get_pg_count "SELECT COUNT(*) FROM users;")
MONGO_USERS=$(get_mongo_count "db.users.countDocuments()")

echo "PostgreSQL users: $PG_USERS"
echo "MongoDB users: $MONGO_USERS"

if [ "$PG_USERS" -eq "$MONGO_USERS" ]; then
    log_success "User count matches"
else
    log_error "User count mismatch: PG=$PG_USERS, Mongo=$MONGO_USERS"
fi

# Check embedded orders count
PG_ORDERS=$(get_pg_count "SELECT COUNT(*) FROM orders;")
TOTAL_EMBEDDED=$(get_mongo_count 'db.users.aggregate([{$project: {orderCount: {$cond: {if: {$isArray: "$orders"}, then: {$size: "$orders"}, else: 0}}}}, {$group: {_id: null, total: {$sum: "$orderCount"}}}]).toArray()[0]?.total || 0')

echo "PostgreSQL orders: $PG_ORDERS"
echo "MongoDB embedded orders: $TOTAL_EMBEDDED"

if [ "$PG_ORDERS" -eq "$TOTAL_EMBEDDED" ] 2>/dev/null; then
    log_success "All orders embedded correctly"
else
    log_error "Embedding mismatch: PG=$PG_ORDERS, Embedded=$TOTAL_EMBEDDED"
fi

# ============================================================================
# TEST 2: Order Embedding - Verify Structure
# ============================================================================
log_test "2" "Order Embedding in User Documents"

EMBEDDED_ORDERS=$(get_mongo_count "db.users.findOne({_id: 1}).orders.length")
PG_USER1_ORDERS=$(get_pg_count "SELECT COUNT(*) FROM orders WHERE user_id = 1;")

echo "User 1 - PostgreSQL orders: $PG_USER1_ORDERS"
echo "User 1 - Embedded orders: $EMBEDDED_ORDERS"

if [ "$PG_USER1_ORDERS" -eq "$EMBEDDED_ORDERS" ]; then
    log_success "Orders correctly embedded in user document"
else
    log_error "Embedding mismatch: PG=$PG_USER1_ORDERS, Embedded=$EMBEDDED_ORDERS"
fi

# ============================================================================
# TEST 3: CDC - INSERT User
# ============================================================================
log_test "3" "CDC INSERT Operation - New User"

BEFORE_COUNT=$(get_mongo_count "db.users.countDocuments()")
echo "Users before insert: $BEFORE_COUNT"

UNIQUE_EMAIL="test.cdc.$(date +%s%N)@example.com"
docker exec postgres-source psql -U postgres -d sourcedb -c \
    "INSERT INTO users (name, email, age, city) VALUES ('Test User CDC', '$UNIQUE_EMAIL', 40, 'TestCity');" > /dev/null

wait_for_cdc 5

AFTER_COUNT=$(get_mongo_count "db.users.countDocuments()")
echo "Users after insert: $AFTER_COUNT"

if [ "$AFTER_COUNT" -eq "$((BEFORE_COUNT + 1))" ]; then
    log_success "User INSERT captured and synced"
else
    log_error "User INSERT failed: expected $((BEFORE_COUNT + 1)), got $AFTER_COUNT"
fi

# ============================================================================
# TEST 4: CDC - UPDATE User
# ============================================================================
log_test "4" "CDC UPDATE Operation - User Info"

docker exec postgres-source psql -U postgres -d sourcedb -c \
    "UPDATE users SET city = 'UpdatedCity', age = 99 WHERE email = '$UNIQUE_EMAIL';" > /dev/null

wait_for_cdc 5

UPDATED_USER=$(get_mongo_count "db.users.findOne({email: '$UNIQUE_EMAIL'}).city")

if [ "$UPDATED_USER" = "UpdatedCity" ]; then
    log_success "User UPDATE captured and synced"
else
    log_error "User UPDATE failed: city is '$UPDATED_USER', expected 'UpdatedCity'"
fi

# ============================================================================
# TEST 5: CDC - INSERT Order with Embedding
# ============================================================================
log_test "5" "CDC INSERT Order - Automatic Embedding"

BEFORE_ORDERS=$(get_mongo_count "db.users.findOne({_id: 1}).orders.length")
echo "User 1 orders before: $BEFORE_ORDERS"

docker exec postgres-source psql -U postgres -d sourcedb -c \
    "INSERT INTO orders (user_id, product_name, quantity, price) VALUES (1, 'Test Product CDC', 5, 599.99);" > /dev/null

wait_for_cdc 5

AFTER_ORDERS=$(get_mongo_count "db.users.findOne({_id: 1}).orders.length")
echo "User 1 orders after: $AFTER_ORDERS"

if [ "$AFTER_ORDERS" -eq "$((BEFORE_ORDERS + 1))" ]; then
    log_success "Order INSERT captured and embedded in user"
else
    log_error "Order embedding failed: expected $((BEFORE_ORDERS + 1)), got $AFTER_ORDERS"
fi

# ============================================================================
# TEST 6: CDC - UPDATE Order in Embedded Document
# ============================================================================
log_test "6" "CDC UPDATE Order - Embedded Document Update"

docker exec postgres-source psql -U postgres -d sourcedb -c \
    "UPDATE orders SET quantity = 999 WHERE product_name = 'Test Product CDC';" > /dev/null

wait_for_cdc 5

UPDATED_QTY=$(get_mongo_count "db.users.findOne({_id: 1}).orders.find(o => o.product_name === 'Test Product CDC').quantity")

if [ "$UPDATED_QTY" = "999" ]; then
    log_success "Order UPDATE reflected in embedded document"
else
    log_error "Embedded order UPDATE failed: quantity is '$UPDATED_QTY', expected 999"
fi

# ============================================================================
# TEST 7: Bulk INSERT Performance
# ============================================================================
log_test "7" "Bulk INSERT Performance (100 records)"

START_TIME=$(date +%s)
BEFORE_BULK=$(get_mongo_count "db.users.countDocuments()")

TIMESTAMP=$(date +%s%N)
docker exec postgres-source psql -U postgres -d sourcedb -c "
DO \$\$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO users (name, email, age, city) 
    VALUES ('BulkUser' || i, 'bulk' || i || '_${TIMESTAMP}@test.com', 20 + (i % 50), 'BulkCity');
  END LOOP;
END \$\$;
" > /dev/null

wait_for_cdc 10

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
AFTER_BULK=$(get_mongo_count "db.users.countDocuments()")
SYNCED=$((AFTER_BULK - BEFORE_BULK))

echo "Time taken: ${DURATION}s"
echo "Records synced: $SYNCED/100"

if [ "$SYNCED" -eq 100 ]; then
    log_success "All 100 records synced (${DURATION}s)"
else
    log_error "Bulk sync incomplete: only $SYNCED/100 records synced"
fi

# ============================================================================
# TEST 8: Data Type Handling - Decimals
# ============================================================================
log_test "8" "Data Type Handling - Decimal/NUMERIC"

# Insert order for user 1 to check decimal in embedded document
docker exec postgres-source psql -U postgres -d sourcedb -c \
    "INSERT INTO orders (user_id, product_name, quantity, price) VALUES (1, 'Decimal Test', 1, 12345.6789);" > /dev/null

wait_for_cdc 5

PRICE=$(get_mongo_count "db.users.findOne({_id: 1}).orders.find(o => o.product_name === 'Decimal Test').price.toString()")

if [[ "$PRICE" == *"12345.67"* || "$PRICE" == *"12345.68"* ]]; then
    log_success "Decimal values preserved correctly in embedded orders"
else
    log_error "Decimal handling failed: got '$PRICE'"
fi

# ============================================================================
# TEST 9: Data Type Handling - Timestamps
# ============================================================================
log_test "9" "Data Type Handling - Timestamps"

TIMESTAMP=$(get_mongo_count "db.users.findOne({_id: 1}).created_at")

if [[ "$TIMESTAMP" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]; then
    log_success "Timestamps converted to ISO-8601 format"
else
    log_error "Timestamp format incorrect: '$TIMESTAMP'"
fi

# ============================================================================
# TEST 10: Referential Integrity - User with Multiple Orders
# ============================================================================
log_test "10" "Referential Integrity - User with Multiple Orders"

UNIQUE_MULTI_EMAIL="multi.$(date +%s%N)@test.com"
docker exec postgres-source psql -U postgres -d sourcedb -c \
    "INSERT INTO users (name, email, age, city) VALUES ('Multi Order User', '$UNIQUE_MULTI_EMAIL', 35, 'TestCity');" > /dev/null

wait_for_cdc 3

NEW_USER_ID=$(get_pg_count "SELECT id FROM users WHERE email = '$UNIQUE_MULTI_EMAIL';")

docker exec postgres-source psql -U postgres -d sourcedb -c \
    "INSERT INTO orders (user_id, product_name, quantity, price) VALUES 
    ($NEW_USER_ID, 'Order 1', 1, 10.00),
    ($NEW_USER_ID, 'Order 2', 2, 20.00),
    ($NEW_USER_ID, 'Order 3', 3, 30.00);" > /dev/null

wait_for_cdc 5

EMBEDDED_COUNT=$(get_mongo_count "db.users.findOne({email: '$UNIQUE_MULTI_EMAIL'}).orders.length")

if [ "$EMBEDDED_COUNT" = "3" ]; then
    log_success "Multiple orders correctly embedded"
else
    log_error "Multiple order embedding failed: expected 3, got $EMBEDDED_COUNT"
fi

# ============================================================================
# TEST 11: Checkpoint Recovery (if Flink restarts)
# ============================================================================
log_test "11" "Flink Job Status & Health"

JOB_STATUS=$(docker exec flink-jobmanager flink list -r 2>/dev/null | grep RUNNING || echo "NONE")

if [[ "$JOB_STATUS" == *"RUNNING"* ]]; then
    log_success "Flink job is running"
else
    log_error "Flink job is not running"
fi

# ============================================================================
# TEST 12: No Data Loss - Final Count Verification
# ============================================================================
log_test "12" "No Data Loss - Final Count Verification"

FINAL_PG_USERS=$(get_pg_count "SELECT COUNT(*) FROM users;")
FINAL_MONGO_USERS=$(get_mongo_count "db.users.countDocuments()")

echo "Final PostgreSQL users: $FINAL_PG_USERS"
echo "Final MongoDB users: $FINAL_MONGO_USERS"

# Check total embedded orders across all users
TOTAL_EMBEDDED=$(get_mongo_count "db.users.aggregate([{\$project: {orderCount: {\$cond: {if: {\$isArray: '\$orders'}, then: {\$size: '\$orders'}, else: 0}}}}, {\$group: {_id: null, total: {\$sum: '\$orderCount'}}}]).toArray()[0]?.total || 0")
FINAL_PG_ORDERS=$(get_pg_count "SELECT COUNT(*) FROM orders;")

echo "Final PostgreSQL orders: $FINAL_PG_ORDERS"
echo "Final embedded orders across all users: $TOTAL_EMBEDDED"

if [ "$FINAL_PG_USERS" -eq "$FINAL_MONGO_USERS" ] && [ "$FINAL_PG_ORDERS" -eq "$TOTAL_EMBEDDED" ]; then
    log_success "No data loss - all records synced and embedded"
else
    log_error "Data inconsistency detected"
fi

# ============================================================================
# FINAL REPORT
# ============================================================================
echo -e "\n${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           TEST SUMMARY                 ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo -e "${BLUE}Total:  $((PASSED + FAILED))${NC}\n"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}\n"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}\n"
    exit 1
fi
