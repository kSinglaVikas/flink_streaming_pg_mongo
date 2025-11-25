#!/bin/bash

# Complete Reset and Restart Script for Flink CDC Pipeline
# Clears all state, resets CDC, and restarts with clean slate

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   FLINK CDC PIPELINE RESET & RESTART  ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# Step 1: Cancel Flink job
echo -e "${YELLOW}[1/8] Cancelling Flink job...${NC}"
JOB_ID=$(docker exec flink-jobmanager flink list -r 2>/dev/null | grep -E '[a-f0-9]{32}' | awk '{print $4}' || echo "")
if [ -n "$JOB_ID" ]; then
    docker exec flink-jobmanager flink cancel $JOB_ID 2>/dev/null || true
    echo -e "${GREEN}✓ Job cancelled${NC}"
else
    echo -e "${YELLOW}⚠ No running job found${NC}"
fi
sleep 2

# Step 2: Stop all containers
echo -e "\n${YELLOW}[2/8] Stopping all containers...${NC}"
docker-compose down
echo -e "${GREEN}✓ Containers stopped${NC}"

# Step 3: Clean Flink state
echo -e "\n${YELLOW}[3/8] Cleaning Flink state and checkpoints...${NC}"
docker volume rm flink-streaming_flink-jobmanager-data flink-streaming_flink-taskmanager-data 2>/dev/null || true
echo -e "${GREEN}✓ Flink state cleaned${NC}"

# Step 4: Clean MongoDB data (optional - comment out if you want to keep MongoDB data)
echo -e "\n${YELLOW}[4/8] Cleaning MongoDB data...${NC}"
docker volume rm flink-streaming_mongodb-data 2>/dev/null || true
echo -e "${GREEN}✓ MongoDB data cleaned${NC}"

# Step 5: Clean PostgreSQL replication slots
echo -e "\n${YELLOW}[5/8] Starting PostgreSQL to clean replication slots...${NC}"
docker-compose up -d postgres
sleep 5

docker exec postgres psql -U postgres -d sourcedb -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'flink';" 2>/dev/null || echo "No replication slot to drop"
echo -e "${GREEN}✓ Replication slots cleaned${NC}"

# Step 6: Start all services
echo -e "\n${YELLOW}[6/8] Starting all services...${NC}"
docker-compose up -d
echo -e "${GREEN}✓ All services starting${NC}"

# Step 7: Wait for services to be ready
echo -e "\n${YELLOW}[7/8] Waiting for services to be ready...${NC}"
sleep 10  # Simple wait for all services to start
echo -e "${GREEN}  ✓ Services should be ready${NC}"

# Step 8: Submit Flink job
echo -e "\n${YELLOW}[8/8] Submitting Flink CDC job...${NC}"
docker exec flink-jobmanager flink run \
    -d \
    /opt/flink/usrlib/postgres-mongodb-pipeline-1.0-SNAPSHOT.jar
echo -e "${GREEN}✓ Flink job submitted${NC}"

sleep 3

# Verify job is running
echo -e "\n${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           VERIFICATION                 ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

sleep 2
JOB_STATUS=$(docker exec flink-jobmanager flink list -r 2>/dev/null | grep RUNNING || echo "")
if [[ "$JOB_STATUS" == *"RUNNING"* ]]; then
    echo -e "${GREEN}✓ Flink job is RUNNING${NC}\n"
else
    echo -e "${YELLOW}⚠ Job may still be initializing${NC}\n"
fi

echo -e "${BLUE}════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Reset and restart complete!${NC}"
echo -e "${BLUE}════════════════════════════════════════${NC}\n"

echo -e "Access points:"
echo -e "  Flink Web UI: ${BLUE}http://localhost:8081${NC}"
echo -e "  PostgreSQL: ${BLUE}localhost:5432${NC}"
echo -e "  MongoDB: ${BLUE}localhost:27017${NC}\n"

echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. Generate test data:"
echo -e "     ${BLUE}./scripts/generate-load.sh 50 5${NC}  # 50 users, 5 orders each"
echo -e "\n  2. Run embedding tests:"
echo -e "     ${BLUE}./scripts/test-embedding.sh${NC}\n"
