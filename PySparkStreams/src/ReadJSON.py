import json

file_path = 'onefilebooking/Booking.json'

def read_json_objects(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            try:
                json_object = json.loads(line.strip())
                print(json_object)  # Process the JSON object as needed
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

if __name__ == '__main__':
    read_json_objects(file_path)