import numpy as np
from dbconnection import get_connection, return_connection

batch_size = 1000  # Adjust the batch size as needed
offset = 0

def get_tensor_shape(tensor_data):
    try:
        tensor_array = np.frombuffer(tensor_data, dtype=np.float32)
        size = tensor_array.size
        
        # Print the size and a sample of the data for debugging
        print(f"Tensor size: {size}")
        print(f"Sample data: {tensor_array[:10]}")  # Print first 10 elements for inspection
        
        # Check if the size matches the expected shape
        expected_size = 25 * 25 * 3  # Adjusted expected size
        if size != expected_size:
            print(f"Unexpected tensor size: {size}, expected: {expected_size}")
            return None
        
        # Reshape the array
        tensor_array = tensor_array.reshape(25, 25, 3)
        return tensor_array.shape
    except ValueError as e:
        print(f"Error: {e}")
        return None

try:
    # Establish the database connection
    conn = get_connection()
    cur = conn.cursor()

    while True:
        # SQL query to select tensors with limit and offset for batching
        query = f"SELECT id, tensor FROM image_tensors ORDER BY id LIMIT {batch_size} OFFSET {offset}"
        cur.execute(query)

        # Fetch the current batch of results
        result = cur.fetchall()

        # Break the loop if no more results
        if not result:
            break

        # Loop through the results and print the shapes of the tensors
        for idx, (id, tensor) in enumerate(result):
            tensor_shape = get_tensor_shape(tensor)
            if tensor_shape:
                print(f"Shape of tensor {offset + idx}: {tensor_shape}")

        # Increment the offset for the next batch
        offset += batch_size

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure the cursor and connection are closed properly
    cur.close()
    return_connection(conn)
