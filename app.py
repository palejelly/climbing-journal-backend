# app.py
import os
import json
import io # Needed for downloading blob content to memory
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__ as azure_storage_version
from azure.core.exceptions import ResourceNotFoundError

# --- Configuration ---
# Load Azure Storage connection string from environment variable
# IMPORTANT: Set this environment variable in your development and deployment environments.
AZURE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
# Define the container name where videos.json is stored
AZURE_CONTAINER_NAME = os.environ.get('AZURE_VIDEO_CONTAINER_NAME', 'climbing-journal-storage') # Default to 'videodata' if not set
# Define the name of the metadata blob

METADATA_BLOB_NAME = 'videos.json'

# Determine the absolute path to the directory this script is in
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# Define the path to the static frontend files (assuming they are in a 'frontend' subdirectory)
STATIC_FOLDER = os.path.join(BASE_DIR, 'frontend')

app = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='')

# --- CORS Configuration ---
CORS(app) # Keep CORS enabled

# --- Helper Function ---
def load_videos_from_azure():
    """Loads video data by downloading videos.json from Azure Blob Storage."""
    if not AZURE_CONNECTION_STRING:
        print("Error: AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
        return ["no videos"] # Return empty list if connection string is missing

    try:
        print(f"Attempting to connect to Azure Storage. SDK Version: {azure_storage_version}")
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

        # Get a client to interact with the specific blob (videos.json)
        blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=METADATA_BLOB_NAME)

        print(f"Attempting to download blob '{METADATA_BLOB_NAME}' from container '{AZURE_CONTAINER_NAME}'...")

        # Download the blob's content
        # download_stream returns a StorageStreamDownloader object
        download_stream = blob_client.download_blob()

        # Read the stream into memory (decode from bytes to string)
        blob_data_bytes = download_stream.readall()
        blob_data_string = blob_data_bytes.decode('utf-8')

        print(f"Successfully downloaded {len(blob_data_bytes)} bytes from blob.")

        # Parse the JSON data
        videos = json.loads(blob_data_string)
        print(f"Successfully parsed JSON data. Found {len(videos)} video entries.")
        return videos

    except ResourceNotFoundError:
        print(f"Error: Blob '{METADATA_BLOB_NAME}' not found in container '{AZURE_CONTAINER_NAME}'.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from downloaded blob '{METADATA_BLOB_NAME}'. Content might be invalid.")
        # Optionally log the first few characters of the downloaded data for debugging:
        # print(f"Downloaded data starts with: {blob_data_string[:100]}")
        return []
    except Exception as e:
        # Catching potential Azure connection errors or other issues
        print(f"An unexpected error occurred interacting with Azure Blob Storage: {e}")
        # Log the type of exception for more specific debugging
        print(f"Exception type: {type(e).__name__}")
        # For more detailed Azure SDK errors, you might need more specific exception handling
        # from azure.core.exceptions import AzureError, ServiceRequestError etc.
        return []


# --- API Routes ---
@app.route('/api/videos', methods=['GET'])
def get_videos():
    """API endpoint to get the list of all videos from Azure Blob."""
    videos = load_videos_from_azure()
    # Ensure the response is always a JSON list, even if loading failed
    return jsonify(videos if isinstance(videos, list) else [])

@app.route('/api/tags', methods=['GET'])
def get_tags():
    """API endpoint to get a list of unique tags from Azure Blob data."""
    videos = load_videos_from_azure()
    all_tags = set()
    # Add extra check to ensure videos is a list before iterating
    if isinstance(videos, list):
        for video in videos:
            # Check if 'tags' key exists and is a list
            if isinstance(video.get('tags'), list):
                 for tag in video['tags']:
                     # Ensure tag is a string before adding
                     if isinstance(tag, str):
                         all_tags.add(tag)
    return jsonify(sorted(list(all_tags)))

# --- Route to serve the frontend ---
# This remains the same, serving index.html from the local 'frontend' folder
@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

# --- Main Execution ---
if __name__ == '__main__':
    # Check if essential config is missing on startup for local dev
    if not AZURE_CONNECTION_STRING:
         print("\n*** WARNING: AZURE_STORAGE_CONNECTION_STRING environment variable is not set. API calls will likely fail. ***\n")

    app.run(host='0.0.0.0', port=3000, debug=True) # Keep debug=True for local dev
