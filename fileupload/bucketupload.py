from google.cloud import storage
import os

def file_upload(bucket_name, source, destination):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination)
        blob.upload_from_filename(source)
        print(f"{source} uploaded to {destination}.")
    except Exception as e:
        print("Error occurred:", e)

bucket_name = "melb-gtfs"
source = "fileupload/2/google_transit/"
destination_prefix = "Metro/"  # GCS folder prefix

for root, _, files in os.walk(source):
    for file in files:
        local_path = os.path.join(root, file)
        
        # Create the relative path for the destination
        relative_path = os.path.relpath(local_path, source)
        gcs_path = os.path.join(destination_prefix, relative_path).replace("\\", "/")
        
        # Upload the file
        file_upload(bucket_name, local_path, gcs_path)
