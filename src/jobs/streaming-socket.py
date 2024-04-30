import json
import socket
import time
import pandas as pd

def send_data_over_socket(file_path, host = "spark-master", port = 9999, chunk_size = 2):
    # Establish a TCP socket connection
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the specified host and port
    s.bind((host, port))
    # # Start listening for incoming connections, with a maximum backlog of 1 connection
    s.listen(1)
    # Print a message indicating the server is listening for connections
    print(f'listening for connections on {host} : {port}')



    # Initialize the index of the last sent line in the file
    last_sent_index = 0

    # We use while true in order for it to reconnect when it deconnects
    while True:
        # Accept a connection from a client, returning a new socket object and the client's address
        conn, address = s.accept()
        print(f"Connection from {address}")

        try:
            with open(file_path, 'r') as file:
                # skip the lines that were already sent
                for lines in range(last_sent_index):
                    next(file)
                
                records = []
                for line in file:
                    records.append(json.loads(line))
                    # Check if enough record have been read to form a chunk
                    if len(records) == chunk_size:
                        chunk = pd.DataFrame(records)
                        print(chunk)
                        # Iterate over each record in the chunk converted to a dictionary format
                        for record in chunk.to_dict(orient = 'records'):
                            # Serialize the record as JSON
                            serialize_data = json.dumps(record).encode('utf-8')
                            # Send the serialized data over the socket, terminated by a newline character
                            conn.send(serialize_data + b"\n")
                            # Pause execution before sending new chunk 
                            time.sleep(5)
                            # Update the index of the last sent line in the file
                            last_sent_index += 1
                        records = []
        except (BrokenPipeError, ConnectionResetError) :
            print("Client disconnected")
        finally:
            conn.close()
            print("Connection closed")


if __name__ == "__main__":
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")
    



