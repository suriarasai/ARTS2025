# stream_generator.py - Taxi Booking Stream Generator
import json
import random
import time
import threading
import socket
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


class BookingStreamSimulator:
    """
    Real-time taxi booking stream simulator using ThreadPoolExecutor
    Generates realistic booking events and streams them via socket
    """

    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
        self.running = False

        # Sample data for realistic booking generation
        self.pickup_districts = [
            "Central", "Orchard", "Marina Bay", "Chinatown", "Little India",
            "Bugis", "Clarke Quay", "Raffles Place", "Sentosa", "Jurong East",
            "Tampines", "Woodlands", "Bedok", "Punggol", "Sengkang", "Toa Payoh",
            "Ang Mo Kio", "Bishan", "Clementi", "Pasir Ris"
        ]

        self.dropoff_districts = [
            "Changi Airport", "Central", "Orchard", "Marina Bay", "Jurong East",
            "Tampines", "Woodlands", "Bedok", "Punggol", "Sengkang", "Toa Payoh",
            "Harbourfront", "Dhoby Ghaut", "City Hall", "Outram Park"
        ]

        # Load reference data (based on provided files)
        self.taxi_numbers = [
            "SHZ2770", "SHY4378", "SHX6464", "SHX4872", "SHX2609",
            "SHY9111", "SHX5867", "SHY6907", "SHX7766", "SHX5844",
            "SHX3617", "SHX7492", "SHX6232", "SHY5882", "SHX7048"
        ]

        self.passenger_ids = list(range(1, 2489))  # Based on passengers CSV (2488 passengers)

        # Taxi types with realistic distribution
        self.taxi_types = [
            ("Standard", 0.5),  # 50% standard taxis
            ("Premier", 0.2),  # 20% premier
            ("Mini Cab", 0.15),  # 15% mini cabs
            ("Maxi Cab", 0.1),  # 10% maxi cabs
            ("Limosine", 0.05)  # 5% limousines
        ]

        # Payment methods
        self.payment_methods = ["Cash", "Card", "Digital Wallet", "Corporate Account"]

        # Time-based patterns for realistic simulation
        self.rush_hours = [(7, 9), (17, 19)]  # Morning and evening rush
        self.night_hours = (22, 6)

    def get_weighted_taxi_type(self):
        """Get taxi type based on realistic distribution"""
        rand = random.random()
        cumulative = 0
        for taxi_type, weight in self.taxi_types:
            cumulative += weight
            if rand <= cumulative:
                return taxi_type
        return "Standard"

    def get_surge_multiplier(self, current_hour):
        """Calculate surge multiplier based on time and randomness"""
        base_surge = 1.0

        # Rush hour surge
        if any(start <= current_hour <= end for start, end in self.rush_hours):
            base_surge = random.uniform(1.3, 2.5)
        # Night time surge
        elif current_hour >= self.night_hours[0] or current_hour <= self.night_hours[1]:
            base_surge = random.uniform(1.1, 1.8)
        # Weekend surge (simplified - random chance)
        elif random.random() < 0.3:
            base_surge = random.uniform(1.2, 2.0)
        else:
            base_surge = random.uniform(1.0, 1.4)

        return round(base_surge, 2)

    def get_realistic_distance(self, pickup, dropoff):
        """Calculate realistic distance based on districts"""
        # Simplified distance calculation
        if pickup == dropoff:
            return round(random.uniform(2.0, 5.0), 2)
        elif "Airport" in dropoff:
            return round(random.uniform(15.0, 35.0), 2)
        elif pickup in ["Central", "Orchard", "Marina Bay"] and dropoff in ["Central", "Orchard", "Marina Bay"]:
            return round(random.uniform(3.0, 8.0), 2)
        else:
            return round(random.uniform(5.0, 25.0), 2)

    def generate_booking(self):
        """Generate a realistic booking event with time-based patterns"""
        current_time = datetime.now()
        current_hour = current_time.hour

        pickup_district = random.choice(self.pickup_districts)
        dropoff_district = random.choice(self.dropoff_districts)

        # Ensure pickup != dropoff
        while dropoff_district == pickup_district:
            dropoff_district = random.choice(self.dropoff_districts)

        estimated_distance = self.get_realistic_distance(pickup_district, dropoff_district)
        surge_multiplier = self.get_surge_multiplier(current_hour)
        requested_taxi_type = self.get_weighted_taxi_type()

        # Passenger count based on taxi type
        if requested_taxi_type == "Mini Cab":
            max_passengers = 3
        elif requested_taxi_type == "Maxi Cab":
            max_passengers = 7
        elif requested_taxi_type == "Limosine":
            max_passengers = 4
        else:
            max_passengers = 4

        num_passengers = random.randint(1, max_passengers)

        booking = {
            "booking_id": f"BK{random.randint(100000, 999999)}",
            "timestamp": current_time.isoformat(),
            "passenger_id": random.choice(self.passenger_ids),
            "pickup_district": pickup_district,
            "dropoff_district": dropoff_district,
            "booking_time": current_time.strftime("%H:%M"),
            "requested_taxi_type": requested_taxi_type,
            "num_passengers": num_passengers,
            "estimated_distance": estimated_distance,
            "surge_multiplier": surge_multiplier,
            "payment_method": random.choice(self.payment_methods),
            "booking_status": "PENDING",
            "special_requirements": random.choice(
                [None, "Child Seat", "Wheelchair Accessible", "Pet Friendly"]) if random.random() < 0.1 else None,
            "priority_level": random.choice(["Normal", "High", "VIP"]) if random.random() < 0.2 else "Normal"
        }
        return booking

    def handle_client(self, client_socket, address, rate_per_second):
        """Handle individual client connection"""
        print(f"Client connected from {address}")

        try:
            while self.running:
                booking = self.generate_booking()
                message = json.dumps(booking) + "\n"

                try:
                    client_socket.send(message.encode('utf-8'))
                    time.sleep(1.0 / rate_per_second)
                except (ConnectionResetError, BrokenPipeError):
                    print(f"Client {address} disconnected")
                    break

        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()

    def stream_bookings(self, rate_per_second=2, max_clients=10):
        """Stream bookings to multiple clients using ThreadPoolExecutor"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(max_clients)
            server_socket.settimeout(1.0)  # Non-blocking accept

            print(f"Booking stream server started on {self.host}:{self.port}")
            print(f"Generating bookings at {rate_per_second} events/second")
            print(f"Maximum {max_clients} concurrent clients supported")

            with ThreadPoolExecutor(max_workers=max_clients) as executor:
                futures = []

                while self.running:
                    try:
                        client_socket, address = server_socket.accept()

                        # Submit client handling to thread pool
                        future = executor.submit(
                            self.handle_client,
                            client_socket,
                            address,
                            rate_per_second
                        )
                        futures.append(future)

                        # Clean up completed futures
                        futures = [f for f in futures if not f.done()]

                    except socket.timeout:
                        continue  # Continue accepting connections
                    except Exception as e:
                        if self.running:
                            print(f"Server error: {e}")
                            time.sleep(1)

        except Exception as e:
            print(f"Failed to start server: {e}")
        finally:
            server_socket.close()
            print("Server socket closed")

    def start(self, rate_per_second=2, max_clients=10):
        """Start the booking stream server"""
        self.running = True

        # Use ThreadPoolExecutor for the main server
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.stream_bookings, rate_per_second, max_clients)
            return future

    def stop(self):
        """Stop the booking stream server"""
        print("Stopping booking stream simulator...")
        self.running = False


def main():
    """Main function for running the stream generator"""
    parser = argparse.ArgumentParser(description="Taxi Booking Stream Generator")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9999, help="Port to bind to")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--max-clients", type=int, default=10, help="Max concurrent clients")
    parser.add_argument("--duration", type=int, help="Run for specified seconds (infinite if not set)")

    args = parser.parse_args()

    # Create and start simulator
    simulator = BookingStreamSimulator(args.host, args.port)

    try:
        print(f"Starting booking stream generator...")
        print(f"Host: {args.host}, Port: {args.port}")
        print(f"Rate: {args.rate} bookings/second")
        print(f"Max clients: {args.max_clients}")

        future = simulator.start(args.rate, args.max_clients)

        if args.duration:
            print(f"Running for {args.duration} seconds...")
            time.sleep(args.duration)
            simulator.stop()
        else:
            print("Running indefinitely. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nReceived interrupt signal")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        simulator.stop()
        print("Stream generator stopped")


if __name__ == "__main__":
    main()

# ==================== USAGE EXAMPLES ====================
"""
USAGE EXAMPLES:

1. Basic usage:
   python stream_generator.py

2. Custom configuration:
   python booking_generator.py --host 0.0.0.0 --port 8888 --rate 5.0

3. Run for specific duration:
   python booking_generator.py --duration 300 --rate 3.0

4. High throughput with multiple clients:
   python booking_generator.py --rate 10.0 --max-clients 20

5. Integration with other components:
   # Terminal 1: Start generator
   python booking_generator.py --rate 2.0

   # Terminal 2: Start processor
   python booking_processor.py --source-host localhost --source-port 9999

FEATURES:
- ThreadPoolExecutor for concurrent client handling
- Realistic time-based booking patterns (rush hours, night surges)
- Weighted taxi type distribution
- Distance calculation based on pickup/dropoff districts
- Support for multiple concurrent clients
- Graceful shutdown and error handling
- Configurable event rates and server parameters
"""