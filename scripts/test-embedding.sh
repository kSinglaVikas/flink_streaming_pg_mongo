#!/bin/bash

echo "🔄 Stopping old job and restarting Flink..."
docker exec flink-jobmanager flink list -r 2>/dev/null | grep -oP '[a-f0-9]{32}' | head -1 | xargs -I {} docker exec flink-jobmanager flink cancel {} 2>/dev/null
sleep 3

echo "♻️  Restarting Flink containers..."
docker restart flink-jobmanager flink-taskmanager
sleep 12

echo "🚀 Submitting new job with order embedding..."
docker exec flink-jobmanager flink run /opt/flink/usrlib/postgres-mongodb-pipeline-1.0-SNAPSHOT.jar

echo ""
echo "⏳ Waiting for data to sync (15 seconds)..."
sleep 15

echo ""
echo "📊 Checking MongoDB - Users with embedded orders:"
echo "=================================================="
docker exec mongodb-target mongosh -u admin -p admin123 --quiet --eval "
use targetdb;
print('\\n👥 Total Users: ' + db.users.countDocuments());
print('📦 Total Orders (separate): ' + db.orders.countDocuments());
print('\\n📝 Sample User with Embedded Orders:\\n');
db.users.findOne({_id: 1});
"

echo ""
echo "✅ Testing complete! Check http://localhost:8081 for Flink UI"
