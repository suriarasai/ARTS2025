#!/bin/bash
# java_fix.sh - Fix Java compatibility issues in running container

echo "🔧 Fixing Java compatibility issues..."

CONTAINER_NAME="pyspark-streaming"

# Check if container is running
if ! podman ps | grep -q $CONTAINER_NAME; then
    echo "❌ Container $CONTAINER_NAME is not running!"
    echo "Please start it with: podman-compose up -d"
    exit 1
fi

echo "📋 Checking Java version in container..."
podman exec $CONTAINER_NAME java -version

echo "🔧 Setting Java environment variables..."
podman exec $CONTAINER_NAME bash -c "
export JAVA_OPTS='--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED'
export SPARK_DRIVER_OPTS='\$JAVA_OPTS'
export SPARK_EXECUTOR_OPTS='\$JAVA_OPTS'
echo 'Java options set successfully'
"

echo "✅ Java compatibility fix applied!"
echo "💡 You can now run your Spark applications."
echo ""
echo "🚀 Try running:"
echo "   podman exec -it $CONTAINER_NAME python src/tumbling_window_consumer.py"