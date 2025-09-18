#!/bin/bash
# run.sh - Easy execution script for the streaming pipeline

set -e

CONTAINER_NAME="pyspark-streaming"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}🚕 $1${NC}"
}

# Function to check if container is running
check_container() {
    if ! podman ps | grep -q $CONTAINER_NAME; then
        print_error "Container $CONTAINER_NAME is not running!"
        echo "Please run: podman-compose up -d"
        exit 1
    fi
}

# Function to check if CSV file exists
check_csv() {
    if [ ! -f "data/RebuTripData.csv" ]; then
        print_error "CSV file not found: data/RebuTripData.csv"
        echo "Please copy your CSV file to the data directory"
        exit 1
    fi
}

# Show usage
show_usage() {
    print_header "Kafka PySpark Streaming Pipeline Runner"
    echo ""
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  setup           - Set up infrastructure"
    echo "  producer        - Start the Kafka producer"
    echo "  tumbling        - Run tumbling window consumer"
    echo "  sliding         - Run sliding window consumer"
    echo "  watermark       - Run watermarking demo"
    echo "  status          - Check services status"
    echo "  logs            - Show container logs"
    echo "  stop            - Stop all services"
    echo "  clean           - Clean up everything"
    echo ""
    echo "Producer options:"
    echo "  producer [events_per_second] [duration_minutes]"
    echo "  Examples:"
    echo "    ./run.sh producer 10 30    # 10 events/sec for 30 minutes"
    echo "    ./run.sh producer 5 60     # 5 events/sec for 1 hour"
    echo ""
}

# Setup infrastructure
setup_infrastructure() {
    print_header "Setting up infrastructure..."

    if podman-compose ps | grep -q "Up"; then
        print_warning "Services are already running"
    else
        print_status "Starting Kafka, Zookeeper, and Kafka UI..."
        podman-compose up -d

        print_status "Waiting for services to be ready..."
        sleep 30

        print_status "Building PySpark container..."
        podman-compose build pyspark-app
    fi

    print_status "✅ Infrastructure is ready!"
    echo ""
    echo "🌐 Services available at:"
    echo "   • Kafka UI: http://localhost:8080"
    echo "   • Kafka Bootstrap: localhost:9092"
}

# Start producer
start_producer() {
    print_header "Starting Kafka Producer..."
    check_container
    check_csv

    EVENTS_PER_SEC=${1:-10}
    DURATION_MIN=${2:-30}

    print_status "Producer settings:"
    echo "   • Events per second: $EVENTS_PER_SEC"
    echo "   • Duration: $DURATION_MIN minutes"
    echo "   • Press Ctrl+C to stop early"
    echo ""

    podman exec -it $CONTAINER_NAME python src/taxi_data_producer.py $EVENTS_PER_SEC $DURATION_MIN
}

# Run tumbling window consumer
run_tumbling() {
    print_header "Starting Tumbling Window Consumer..."
    check_container

    print_status "Running tumbling window analysis..."
    echo "   • 2-minute trip count and revenue windows"
    echo "   • 3-minute district popularity windows"
    echo "   • 5-minute taxi type performance windows"
    echo "   • Press Ctrl+C to stop"
    echo ""

    podman exec -it $CONTAINER_NAME python src/tumbling_window_consumer.py
}

# Run sliding window consumer
run_sliding() {
    print_header "Starting Sliding Window Consumer..."
    check_container

    print_status "Running sliding window analysis..."
    echo "   • 5-min window, 1-min slide: Trip trends"
    echo "   • 4-min window, 30-sec slide: District popularity"
    echo "   • 3-min window, 30-sec slide: Taxi performance"
    echo "   • 6-min window, 2-min slide: Rush hour detection"
    echo "   • Press Ctrl+C to stop"
    echo ""

    podman exec -it $CONTAINER_NAME python src/sliding_window_consumer.py
}

# Run watermarking demo
run_watermark() {
    print_header "Starting Watermarking Demo..."
    check_container

    print_status "Running watermarking comparison..."
    echo "   • STRICT (30s): Fast processing, may drop late data"
    echo "   • LENIENT (5m): Accepts more late data, uses more memory"
    echo "   • NO WATERMARK: Keeps everything, potential memory issues"
    echo "   • LATE DATA ANALYSIS: Categorizes data by lateness"
    echo "   • Press Ctrl+C to stop"
    echo ""

    podman exec -it $CONTAINER_NAME python src/watermarking_demo.py
}

# Check status
check_status() {
    print_header "Services Status"
    echo ""

    echo "📊 Container Status:"
    podman-compose ps
    echo ""

    echo "📈 Resource Usage:"
    podman stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
    echo ""

    if podman ps | grep -q kafka; then
        echo "🔌 Kafka Topics:"
        podman exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "Cannot connect to Kafka"
    fi
}

# Show logs
show_logs() {
    SERVICE=${1:-pyspark-streaming}
    print_header "Showing logs for: $SERVICE"
    podman logs -f $SERVICE
}

# Stop services
stop_services() {
    print_header "Stopping all services..."
    podman-compose down
    print_status "✅ All services stopped"
}

# Clean up everything
clean_up() {
    print_header "Cleaning up everything..."

    print_status "Stopping containers..."
    podman-compose down -v

    print_status "Removing volumes..."
    podman volume prune -f

    print_status "Removing images..."
    podman rmi kafka-pyspark-streaming_pyspark-app 2>/dev/null || true

    print_status "✅ Cleanup complete"
}

# Main script logic
case "${1:-help}" in
    "setup")
        setup_infrastructure
        ;;
    "producer")
        start_producer $2 $3
        ;;
    "tumbling")
        run_tumbling
        ;;
    "sliding")
        run_sliding
        ;;
    "watermark")
        run_watermark
        ;;
    "status")
        check_status
        ;;
    "logs")
        show_logs $2
        ;;
    "stop")
        stop_services
        ;;
    "clean")
        clean_up
        ;;
    "help"|*)
        show_usage
        ;;
esac