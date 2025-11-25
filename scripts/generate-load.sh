#!/bin/bash

# Load Generator for Flink CDC Pipeline
# Generates realistic test data to validate CDC and embedding at scale

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

USERS_TO_CREATE=${1:-50}
ORDERS_PER_USER=${2:-5}

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║       FLINK CDC LOAD GENERATOR        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Users to create: $USERS_TO_CREATE"
echo -e "  Orders per user: $ORDERS_PER_USER"
echo -e "  Total operations: $((USERS_TO_CREATE + USERS_TO_CREATE * ORDERS_PER_USER))\n"

# Arrays for realistic test data
FIRST_NAMES=("John" "Jane" "Michael" "Sarah" "David" "Emily" "James" "Emma" "Robert" "Olivia" 
             "William" "Ava" "Richard" "Sophia" "Joseph" "Isabella" "Thomas" "Mia" "Charles" "Charlotte")
LAST_NAMES=("Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis" "Rodriguez" "Martinez"
            "Hernandez" "Lopez" "Gonzalez" "Wilson" "Anderson" "Thomas" "Taylor" "Moore" "Jackson" "Martin")
CITIES=("New York" "Los Angeles" "Chicago" "Houston" "Phoenix" "Philadelphia" "San Antonio" "San Diego" 
        "Dallas" "San Jose" "Austin" "Jacksonville" "Fort Worth" "Columbus" "Charlotte")
PRODUCTS=("Laptop" "Smartphone" "Tablet" "Headphones" "Keyboard" "Mouse" "Monitor" "Webcam" "Speaker" 
          "Charger" "USB Cable" "Power Bank" "Desk Lamp" "Mouse Pad" "Laptop Stand" "HDMI Cable" 
          "External SSD" "Wireless Earbuds" "Microphone" "Docking Station")

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Phase 1: Creating Users${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"

START_TIME=$(date +%s)

# Generate users in batches for better performance
BATCH_SIZE=10
for ((batch=0; batch<$USERS_TO_CREATE; batch+=$BATCH_SIZE)); do
    SQL_BATCH=""
    for ((i=0; i<$BATCH_SIZE && batch+i<$USERS_TO_CREATE; i++)); do
        FIRST=${FIRST_NAMES[$RANDOM % ${#FIRST_NAMES[@]}]}
        LAST=${LAST_NAMES[$RANDOM % ${#LAST_NAMES[@]}]}
        AGE=$((20 + RANDOM % 50))
        CITY=${CITIES[$RANDOM % ${#CITIES[@]}]}
        EMAIL=$(echo "$FIRST.$LAST" | tr '[:upper:]' '[:lower:]')_$(date +%s%N | cut -b1-13)@example.com
        
        SQL_BATCH+="INSERT INTO users (name, email, age, city) VALUES ('$FIRST $LAST', '$EMAIL', $AGE, '$CITY');"
    done
    
    docker exec postgres-source psql -U postgres -d sourcedb -c "$SQL_BATCH" > /dev/null 2>&1
    echo -e "${GREEN}✓${NC} Created users batch $((batch+1))-$((batch+i)) of $USERS_TO_CREATE"
done

USER_END=$(date +%s)
echo -e "\n${GREEN}✓ Users created in $((USER_END - START_TIME))s${NC}\n"

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Phase 2: Creating Orders${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"

# Get only the newly created user IDs (based on timestamp or recent IDs)
MIN_USER_ID=$(docker exec postgres-source psql -U postgres -d sourcedb -t -c "SELECT MAX(id) - $USERS_TO_CREATE + 1 FROM users;" | xargs)
USER_IDS=($(docker exec postgres-source psql -U postgres -d sourcedb -t -c "SELECT id FROM users WHERE id >= $MIN_USER_ID ORDER BY id;" | xargs))

TOTAL_ORDERS=$((${#USER_IDS[@]} * ORDERS_PER_USER))
ORDER_COUNT=0

for USER_ID in "${USER_IDS[@]}"; do
    SQL_BATCH=""
    for ((j=0; j<$ORDERS_PER_USER; j++)); do
        PRODUCT=${PRODUCTS[$RANDOM % ${#PRODUCTS[@]}]}
        QUANTITY=$((1 + RANDOM % 5))
        PRICE=$(awk -v min=10 -v max=2000 'BEGIN{srand(); print min+rand()*(max-min)}')
        PRICE=$(printf "%.2f" $PRICE)
        
        SQL_BATCH+="INSERT INTO orders (user_id, product_name, quantity, price) VALUES ($USER_ID, '$PRODUCT', $QUANTITY, $PRICE);"
        ORDER_COUNT=$((ORDER_COUNT + 1))
    done
    
    docker exec postgres-source psql -U postgres -d sourcedb -c "$SQL_BATCH" > /dev/null 2>&1
    
    # Progress indicator every 10 users
    if [ $((ORDER_COUNT % 50)) -eq 0 ]; then
        echo -e "${GREEN}✓${NC} Created $ORDER_COUNT / $TOTAL_ORDERS orders"
    fi
done

ORDER_END=$(date +%s)
echo -e "\n${GREEN}✓ Orders created in $((ORDER_END - USER_END))s${NC}\n"

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo -e "\n${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         LOAD GENERATION SUMMARY        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# Verify results
PG_USERS=$(docker exec postgres-source psql -U postgres -d sourcedb -t -c "SELECT COUNT(*) FROM users;" | xargs)
MONGO_USERS=$(docker exec mongodb-target mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db = db.getSiblingDB('targetdb'); db.users.countDocuments()")
PG_ORDERS=$(docker exec postgres-source psql -U postgres -d sourcedb -t -c "SELECT COUNT(*) FROM orders;" | xargs)
TOTAL_EMBEDDED=$(docker exec mongodb-target mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db = db.getSiblingDB('targetdb'); db.users.aggregate([{\$project: {orderCount: {\$cond: {if: {\$isArray: '\$orders'}, then: {\$size: '\$orders'}, else: 0}}}}, {\$group: {_id: null, total: {\$sum: '\$orderCount'}}}]).toArray()[0]?.total || 0")

echo -e "${YELLOW}PostgreSQL:${NC}"
echo -e "  Users: $PG_USERS"
echo -e "  Orders: $PG_ORDERS\n"

echo -e "${YELLOW}MongoDB:${NC}"
echo -e "  Users: $MONGO_USERS"
echo -e "  Embedded Orders: $TOTAL_EMBEDDED"
echo -e "  Sync Status: $TOTAL_EMBEDDED / $PG_ORDERS orders embedded ($(echo "scale=1; $TOTAL_EMBEDDED*100/$PG_ORDERS" | bc)%)\n"

echo -e "${YELLOW}Performance:${NC}"
echo -e "  Total time: ${TOTAL_DURATION}s"
echo -e "  Users/sec: $(echo "scale=2; $PG_USERS/$TOTAL_DURATION" | bc)"
echo -e "  Orders/sec: $(echo "scale=2; $PG_ORDERS/$TOTAL_DURATION" | bc)\n"

if [ "$PG_USERS" -eq "$MONGO_USERS" ] && [ "$PG_ORDERS" -eq "$TOTAL_EMBEDDED" ]; then
    echo -e "${GREEN}✓ All data synced and embedded successfully!${NC}\n"
    exit 0
else
    echo -e "${YELLOW}⚠ CDC still processing - run validation:${NC}"
    echo -e "   ${BLUE}./scripts/test-embedding.sh${NC}\n"
    exit 0
fi
