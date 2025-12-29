# app.py
import os
import json
import io # Needed for downloading blob content to memory
import uuid # For generating unique blob names
import tempfile # For creating temporary directories/files
import shutil # For removing temporary directories
from flask import Flask, jsonify, send_from_directory, request

from werkzeug.security import generate_password_hash, check_password_hash

# --- Configuration ---
# Load Azure Storage connection string from environment variable
# IMPORTANT: Set this environment variable in your development and deployment environments.
AZURE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
# this is connection string for azure storage account. 

# Define the container name where videos.json is stored
AZURE_METADATA_CONTAINER_NAME = os.environ.get('AZURE_METADATA_CONTAINER_NAME', 'climbing-journal-storage') # Default to 'videodata' if not set
# Define the name of the metadata blob

# Define the container name where actual video files will be uploaded
AZURE_VIDEO_FILES_CONTAINER_NAME = os.environ.get('AZURE_VIDEO_FILES_CONTAINER_NAME', 'climbing-journal-videos') # Default 'videos'
AZURE_THUMBNAILS_CONTAINER_NAME = os.environ.get('AZURE_THUMBNAILS_CONTAINER_NAME', 'climbing-journal-thumbnails')


METADATA_BLOB_NAME = 'videos.json'
USER_METADATA_BLOB_NAME = 'users.json'


# NEW: Thumbnail generation settings
THUMBNAIL_TIME_SECONDS = 5.0 # Time in seconds to grab the frame
THUMBNAIL_FILENAME_SUFFIX = '_thumb.jpg' # Suffix for thumbnail files
THUMBNAIL_CONTENT_TYPE = 'image/jpeg'

# Determine the absolute path to the directory this script is in
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# Define the path to the static frontend files (assuming they are in a 'frontend' subdirectory)
STATIC_FOLDER = os.path.join(BASE_DIR, 'frontend')

# JUNHO TODO : this part needs to be studied. 
app = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='')

try: 
    from flask_cors import CORS
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings, __version__ as azure_storage_version
    from azure.core.exceptions import ResourceNotFoundError
    from moviepy import VideoFileClip # Import moviepy

    # --- CORS Configuration ---
    CORS(app) # Keep CORS enabled
except:
    pass

# --- Helper Function ---
def get_blob_service_client():
    """Creates and returns a BlobServiceClient if connection string is available."""
    if not AZURE_CONNECTION_STRING:
        print("Error: AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
        return None
    try:
        return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    except Exception as e:
        print(f"Error creating BlobServiceClient: {e}")
        return None

def load_videos_from_azure(blob_service_client):
    """Loads video metadata by downloading videos.json from Azure Blob Storage."""
    if not blob_service_client:
        return []

    try:
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_METADATA_CONTAINER_NAME, blob=METADATA_BLOB_NAME
        )
        print(f"Attempting to download blob '{METADATA_BLOB_NAME}' from container '{AZURE_METADATA_CONTAINER_NAME}'...")
        download_stream = blob_client.download_blob()
        blob_data_bytes = download_stream.readall()
        blob_data_string = blob_data_bytes.decode('utf-8')
        print(f"Successfully downloaded metadata blob.")
        videos = json.loads(blob_data_string)
        print(f"Successfully parsed JSON data. Found {len(videos)} video entries.")
        return videos
    
    except ResourceNotFoundError:
        print(f"Metadata blob '{METADATA_BLOB_NAME}' not found in container '{AZURE_METADATA_CONTAINER_NAME}'. Returning empty list.")
        return [] # If metadata doesn't exist yet, start fresh
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from downloaded blob '{METADATA_BLOB_NAME}'.")
        return None # Indicate error by returning None
    except Exception as e:
        print(f"An unexpected error occurred loading video metadata: {e}")
        return None # Indicate error

def save_videos_to_azure(blob_service_client, videos_data):
    """Uploads the updated video metadata list back to videos.json in Azure Blob Storage."""
    if not blob_service_client:
        return False

    try:
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_METADATA_CONTAINER_NAME, blob=METADATA_BLOB_NAME
        )
        # Convert Python list back to JSON string
        updated_json_data = json.dumps(videos_data, indent=2) # Use indent for readability
        # Upload the JSON string, overwriting the existing blob
        blob_client.upload_blob(updated_json_data.encode('utf-8'), overwrite=True,
                                content_settings=ContentSettings(content_type='application/json'))
        print(f"Successfully uploaded updated metadata to blob '{METADATA_BLOB_NAME}'.")
        return True
    except Exception as e:
        print(f"An unexpected error occurred saving video metadata: {e}")
        return False

def upload_blob_to_azure(blob_service_client, container_name, blob_name, file_stream, content_type):
    """Uploads a file stream to a specified blob."""
    if not blob_service_client: return None
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        print(f"Uploading blob '{blob_name}' to container '{container_name}' with content type '{content_type}'...")
        blob_client.upload_blob(file_stream, overwrite=True, content_settings=ContentSettings(content_type=content_type))
        print(f"Blob '{blob_name}' upload successful.")
        return blob_client.url # Return the URL of the uploaded blob
    except Exception as e:
        print(f"Error uploading blob '{blob_name}' to container '{container_name}': {e}")
        return None

import subprocess

def generate_thumbnail(video_temp_path, thumb_temp_path, timestamp_sec):
    """
    Generates a thumbnail using a direct FFmpeg call to bypass 
    MoviePy 2.2.1 metadata parsing errors.
    """
    try:
        # Construct the command
        # -ss BEFORE -i is much faster (fast-seek)
        cmd = [
            'ffmpeg',
            '-y',                 # Overwrite output file if it exists
            '-ss', str(timestamp_sec), 
            '-i', video_temp_path, 
            '-vframes', '1',      # Output exactly one frame
            '-q:v', '2',          # Quality (2-5 is high quality)
            thumb_temp_path
        ]
        
        # Run the command
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Thumbnail successfully generated at {thumb_temp_path}")
            return True
        else:
            print(f"FFmpeg Error output: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Direct FFmpeg call failed: {e}")
        return False

# --- NEW: Helper Functions for User Data ---
def load_users_from_azure(blob_service_client):
    """Loads user data from users.json in Azure Blob Storage."""
    if not blob_service_client: return []
    try:
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_METADATA_CONTAINER_NAME, blob=USER_METADATA_BLOB_NAME
        )
        print(f"Attempting to download blob '{USER_METADATA_BLOB_NAME}' from container '{AZURE_METADATA_CONTAINER_NAME}'...")
        download_stream = blob_client.download_blob()
        users = json.loads(download_stream.readall())
        print(f"Successfully downloaded and parsed user data.")
        return users
    except ResourceNotFoundError:
        print(f"User data blob '{USER_METADATA_BLOB_NAME}' not found. Starting with an empty list.")
        return [] # If no users file exists, start with an empty list
    except Exception as e:
        print(f"An error occurred loading user data: {e}")
        return None # Indicate error

def save_users_to_azure(blob_service_client, users_data):
    """Uploads the updated user list back to users.json in Azure Blob Storage."""
    if not blob_service_client: return False
    try:
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_METADATA_CONTAINER_NAME, blob=USER_METADATA_BLOB_NAME
        )
        updated_json_data = json.dumps(users_data, indent=2)
        blob_client.upload_blob(updated_json_data.encode('utf-8'), overwrite=True,
                                content_settings=ContentSettings(content_type='application/json'))
        print(f"Successfully uploaded updated user data to blob '{USER_METADATA_BLOB_NAME}'.")
        return True
    except Exception as e:
        print(f"An error occurred saving user data: {e}")
        return False


# --- API Routes ---

@app.route('/auth/join', methods=['POST'])
def handle_join():
    """API endpoint for user registration (signing up)."""
    data = request.get_json()
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({"error": "Username and password are required"}), 400

    username = data['username']
    password = data['password']

    blob_service_client = get_blob_service_client()
    if not blob_service_client:
        return jsonify({"error": "Azure Storage connection not configured"}), 500

    users = load_users_from_azure(blob_service_client)
    if users is None:
        return jsonify({"error": "Failed to load user data"}), 500

    # Check if username already exists
    if any(user['username'] == username for user in users):
        return jsonify({"error": "Username already exists"}), 409 # 409 Conflict

    # Hash the password for secure storage
    hashed_password = generate_password_hash(password)

    # Add new user
    new_user = {"username": username, "password": hashed_password}
    users.append(new_user)

    # Save updated user list back to Azure
    if not save_users_to_azure(blob_service_client, users):
        return jsonify({"error": "Failed to save new user data"}), 500

    return jsonify({"message": f"User '{username}' created successfully"}), 201


@app.route('/auth/login', methods=['POST'])
def handle_login():

    # need to update join/login to sql server.
    """API endpoint for user authentication (signing in)."""
    data = request.get_json()
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({"error": "Username and password are required"}), 400

    username = data['username']
    password = data['password']

    blob_service_client = get_blob_service_client()
    if not blob_service_client:
        return jsonify({"error": "Azure Storage connection not configured"}), 500

    users = load_users_from_azure(blob_service_client)
    if users is None:
        return jsonify({"error": "Failed to load user data"}), 500

    # Find the user
    user = next((user for user in users if user['username'] == username), None)

    # Check if user exists and if the password is correct
    if user and check_password_hash(user['password'], password):
        # In a real app, you would generate and return a JWT (JSON Web Token) here.
        # For simplicity, we'll just return a success message.
        return jsonify({"message": f"Login successful for user '{username}'"}), 200
    else:
        return jsonify({"error": "Invalid username or password"}), 401 # 401 Unauthorized



@app.route('/api/videos', methods=['GET'])
def get_videos():
    """API endpoint to get the list of all videos from Azure Blob."""
    blob_service_client = get_blob_service_client()
    videos = load_videos_from_azure(blob_service_client)
    if videos is None: # Check for loading error
         return jsonify({"error": "Failed to load video metadata"}), 500
    return jsonify(videos)


@app.route('/api/tags', methods=['GET'])
def get_tags():
    """API endpoint to get a list of unique tags from Azure Blob data."""
    blob_service_client = get_blob_service_client()
    videos = load_videos_from_azure(blob_service_client)
    if videos is None: return jsonify({"error": "Failed to load video metadata"}), 500
    all_tags = set()

    if isinstance(videos, list):
        for video in videos:
            if isinstance(video.get('tags'), list):
                 for tag in video['tags']:
                     if isinstance(tag, str): all_tags.add(tag)
    return jsonify(sorted(list(all_tags)))

# uploading video
@app.route('/api/upload', methods=['POST'])
def upload_video():
    """API endpoint to upload video, generate thumbnail, and update metadata."""
    temp_dir = None # Initialize temporary directory path
    video_temp_path = None
    thumb_temp_path = None

    try:
        # --- 1. Prerequisites and Get Form Data ---

        # COMMENT THIS PART FOR LOCAL DEV
        blob_service_client = get_blob_service_client()
        if not blob_service_client:
            return jsonify({"error": "Azure Storage connection not configured"}), 500

        if 'videoFile' not in request.files:
            return jsonify({"error": "No video file part in the request"}), 400
        

        file = request.files['videoFile']
        if file.filename == '':
            return jsonify({"error": "No selected video file"}), 400

        title = request.form.get('title', 'Untitled Video')

        # LET's add error handling to the form.
        user_id = request.form.get('user_id')
        
        tags_string = request.form.get('tags', '')
        tags_list = [tag.strip() for tag in tags_string.split(',') if tag.strip()]

        # --- 2. Save Video Temporarily ---
        # Create a temporary directory to store files during processing
        temp_dir = tempfile.mkdtemp(prefix='upload_')
        print(f"Created temporary directory: {temp_dir}")

        # Sanitize filename slightly (replace spaces, etc.) - more robust sanitization might be needed
        safe_filename = file.filename.replace(" ", "_")
        video_temp_path = os.path.join(temp_dir, safe_filename)
        file.save(video_temp_path)
        print(f"Video saved temporarily to: {video_temp_path}")

        # --- 3. Generate Thumbnail ---
        base_name = os.path.splitext(safe_filename)[0]
        thumb_temp_filename = f"{base_name}{THUMBNAIL_FILENAME_SUFFIX}"
        thumb_temp_path = os.path.join(temp_dir, thumb_temp_filename)

        if not generate_thumbnail(video_temp_path, thumb_temp_path, THUMBNAIL_TIME_SECONDS):
            # If thumbnail generation fails, proceed without it or return error?
            # For now, let's proceed but log the issue and use a placeholder URL.
            print("Thumbnail generation failed. Proceeding without custom thumbnail.")
            thumbnail_url = "https://placehold.co/600x400/fecaca/1f2937?text=Thumb+Error" # Placeholder on error
            # Optionally, delete the failed (empty?) thumbnail file if it exists
            if os.path.exists(thumb_temp_path):
                os.remove(thumb_temp_path)
            thumb_temp_path = None # Ensure we don't try to upload it later
        else:
             # --- 4. Upload Thumbnail ---
             thumb_blob_name = f"{uuid.uuid4()}{THUMBNAIL_FILENAME_SUFFIX}" # Unique name for blob
             with open(thumb_temp_path, 'rb') as thumb_file_stream:
                 thumbnail_url = upload_blob_to_azure(
                     blob_service_client,
                     AZURE_THUMBNAILS_CONTAINER_NAME,
                     thumb_blob_name,
                     thumb_file_stream,
                     THUMBNAIL_CONTENT_TYPE
                 )
             if not thumbnail_url:
                 # Handle thumbnail upload failure - maybe use placeholder?
                 print("Thumbnail upload failed. Using placeholder.")
                 thumbnail_url = "https://placehold.co/600x400/fbbf24/1f2937?text=Upload+Error"


        # --- 5. Upload Original Video File ---
        video_blob_name = f"{uuid.uuid4()}{os.path.splitext(safe_filename)[1]}" # Unique name
        video_content_type = file.content_type or 'application/octet-stream'
        with open(video_temp_path, 'rb') as video_file_stream:
            video_url = upload_blob_to_azure(
                blob_service_client,
                AZURE_VIDEO_FILES_CONTAINER_NAME,
                video_blob_name,
                video_file_stream,
                video_content_type
            )
        if not video_url:
            return jsonify({"error": "Failed to upload video file to Azure Storage"}), 500


        # --- 6. Update Metadata ---
        videos_metadata = load_videos_from_azure(blob_service_client)
        if videos_metadata is None:
            return jsonify({"error": "Failed to load existing metadata before update"}), 500

        next_id = max([v.get('id', 0) for v in videos_metadata]) + 1 if videos_metadata else 1
        new_video_entry = {
            "id": next_id,
            "title": title,
            "thumbnail": thumbnail_url, # Use the generated (or placeholder) thumbnail URL
            "videoUrl": video_url,
            "tags": tags_list,
            "user_id": user_id,
        }
        videos_metadata.append(new_video_entry)

        if not save_videos_to_azure(blob_service_client, videos_metadata):
            # TODO: Consider rolling back blob uploads if metadata save fails
            return jsonify({"error": "Failed to save updated metadata to Azure Storage"}), 500

        # --- 7. Success Response ---
        print(f"Successfully processed upload for video ID {next_id}: {title}")
        return jsonify({
            "message": "Video uploaded and metadata updated successfully!",
            "newVideo": new_video_entry
            }), 201

    except Exception as e:
        # Catch any unexpected errors during the process
        print(f"An error occurred during the upload process: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        return jsonify({"error": "An internal server error occurred during upload."}), 500

    finally:
        # --- 8. Cleanup Temporary Files ---
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir) # Remove the directory and all its contents
                print(f"Successfully removed temporary directory: {temp_dir}")
            except Exception as e:
                print(f"Error removing temporary directory {temp_dir}: {e}")


# --- Route to serve the frontend ---
# This remains the same, serving index.html from the local 'frontend' folder
@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/join')
def serve_join():
    return send_from_directory(app.static_folder, 'join.html')

# --- Main Execution ---
if __name__ == '__main__':
    # Check if essential config is missing on startup for local dev
    if not AZURE_CONNECTION_STRING:
         print("\n*** WARNING: AZURE_STORAGE_CONNECTION_STRING environment variable is not set. API calls will likely fail. ***\n")

    app.run(host='0.0.0.0', port=3000, debug=True) # Keep debug=True for local dev
