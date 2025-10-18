import socket
import time
import sys

# A simple script to wait for a network service to become available.

if len(sys.argv) != 3:
    print("Usage: python wait_for_service.py <host> <port>")
    sys.exit(1)

host = sys.argv[1]
port = int(sys.argv[2])
retries = 30 # Try for 60 seconds

print(f"Waiting for {host}:{port} to become available...")

for i in range(retries):
    try:
        # Try to create a connection
        with socket.create_connection((host, port), timeout=5):
            print(f"{host}:{port} is available! Proceeding.")
            sys.exit(0) # Exit with success code
    except (socket.timeout, ConnectionRefusedError, socket.gaierror, OSError) as e:
        print(f"Attempt {i+1}/{retries}: {host}:{port} not ready yet ({e}), retrying in 2 seconds...")
        time.sleep(2)

print(f"Error: Service {host}:{port} did not become available after {retries * 2} seconds.")
sys.exit(1) # Exit with failure code