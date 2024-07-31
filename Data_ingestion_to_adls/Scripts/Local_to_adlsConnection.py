from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import schedule
import time

# Set up your Azure Blob Storage connection and container details
connect_str = "NOT_allowed_on_GIThub"
container_name = "global-bank"
directory_name_in_container = "Transactions"  # The name of the directory in the container
local_folder = r"C:\Users\sahill\Desktop\Big data\CapstoneProject\Uploader"
checkpoint_file = r"C:\Users\sahill\Desktop\Big data\CapstoneProject\upload_checkpoint.txt"  # Checkpoint file

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

def read_checkpoint():
    """Read the checkpoint file and return a set of uploaded file names."""
    if not os.path.exists(checkpoint_file):
        return set()
    with open(checkpoint_file, 'r') as f:
        return set(line.strip() for line in f)

def write_checkpoint(uploaded_files):
    """Write the list of uploaded files to the checkpoint file."""
    with open(checkpoint_file, 'a') as f:
        for file_name in uploaded_files:
            f.write(f"{file_name}\n")

def upload_files():
    try:
        uploaded_files = read_checkpoint()
        new_uploads = []
        
        for root, dirs, files in os.walk(local_folder):
            for file_name in files:
                if file_name in uploaded_files:
                    continue  # Skip files that have already been uploaded

                file_path = os.path.join(root, file_name)
                
                # Get the relative path of the file from the local_folder
                relative_path = os.path.relpath(file_path, local_folder)
                
                # Prepend the directory name in the container to the relative path
                blob_path = os.path.join(directory_name_in_container, relative_path)
                
                # Use the blob_path as the blob name to maintain the directory structure
                blob_client = container_client.get_blob_client(blob_path)
                
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                new_uploads.append(file_name)
                print(f"Uploaded {file_name} to {container_name}/{blob_path}")

        if new_uploads:
            write_checkpoint(new_uploads)

    except Exception as ex:
        print(f"Exception: {ex}")

# Schedule the task
schedule.every(30).seconds.do(upload_files)

print("Scheduler started. Press Ctrl+C to exit.")
while True:
    schedule.run_pending()
    time.sleep(1)
