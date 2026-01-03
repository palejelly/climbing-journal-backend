# app.py
import os
import threading # <---- import to process the video
import subprocess

from dotenv import load_dotenv  # <--- Import this


load_dotenv()

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

VIDEO_PROCESSING_TIMEOUT = 300  # 300 seconds (5 minutes)

# Determine the absolute path to the directory this script is in
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# Define the path to the static frontend files (assuming they are in a 'frontend' subdirectory)
STATIC_FOLDER = os.path.join(BASE_DIR, 'frontend')

app = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='')

video_cache = None  # Global cache variable

try: 
    from flask_cors import CORS
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings, __version__ as azure_storage_version
    from azure.core.exceptions import ResourceNotFoundError

    # --- CORS Configuration ---
    CORS(app) # Keep CORS enabled
except:
    pass


def background_video_processing(video_id, input_temp_path, safe_filename):
    """
    Handles the heavy lifting in the background with TIMEOUT protection.
    """
    blob_service_client = get_blob_service_client()
    # We define temp_dir here so it's available in the finally block
    temp_dir = os.path.dirname(input_temp_path) 
    
    try:
        base_name = os.path.splitext(safe_filename)[0]
        
        # Paths for processed files
        processed_video_path = os.path.join(temp_dir, f"{base_name}_1080p.mp4")
        thumb_temp_path = os.path.join(temp_dir, f"{base_name}_thumb.jpg")

        # 1. SCALE AND RE-ENCODE (FFmpeg)
        # This fixes mobile compatibility and reduces file size
        encode_cmd = [
            'ffmpeg', '-y', '-i', input_temp_path,
            '-vf', "scale='min(1920,iw)':-2", 
            '-vcodec', 'libx264', '-crf', '23', '-preset', 'medium',
            '-acodec', 'aac', '-movflags', 'faststart',
            processed_video_path
        ]
        
        print(f"Starting FFmpeg for video {video_id} with {VIDEO_PROCESSING_TIMEOUT}s timeout...")
        
        # --- NEW: Added timeout parameter ---
        subprocess.run(
            encode_cmd, 
            check=True, 
            capture_output=True, 
            timeout=VIDEO_PROCESSING_TIMEOUT # <--- Enforces the time limit
        )

        # 2. GENERATE THUMBNAIL
        generate_thumbnail(processed_video_path, thumb_temp_path, 1.0)

        # 3. UPLOAD TO AZURE
        # Upload Processed Video
        video_blob_name = f"{uuid.uuid4()}.mp4"
        with open(processed_video_path, 'rb') as v_file:
            video_url = upload_blob_to_azure(
                blob_service_client, 
                AZURE_VIDEO_FILES_CONTAINER_NAME,
                video_blob_name, 
                v_file, 
                'video/mp4' 
            )

        # Upload Thumbnail
        thumb_blob_name = f"{uuid.uuid4()}.jpg"
        with open(thumb_temp_path, 'rb') as t_file:
            thumb_url = upload_blob_to_azure(
                blob_service_client, AZURE_THUMBNAILS_CONTAINER_NAME,
                thumb_blob_name, t_file, 'image/jpeg'
            )

        # 4. UPDATE METADATA STATUS
        for attempt in range(3):
            videos_metadata = load_videos_from_azure(blob_service_client, force_refresh=True)
            for v in videos_metadata:
                if v['id'] == video_id:
                    v['videoUrl'] = video_url
                    v['thumbnail'] = thumb_url
                    v['status'] = 'completed'
                    break
            if save_videos_to_azure(blob_service_client, videos_metadata):
                break 

    # --- NEW: Catch Timeout Specifically ---
    except subprocess.TimeoutExpired as e:
        print(f"!!! TIMEOUT: Video {video_id} took longer than {VIDEO_PROCESSING_TIMEOUT} seconds.")
        # Update status to 'failed' (or you could add a specific 'timeout' status)
        videos_metadata = load_videos_from_azure(blob_service_client)
        if videos_metadata:
            for v in videos_metadata:
                if v['id'] == video_id:
                    v['status'] = 'timeout' # Letting the user know it timed out
                    break
            save_videos_to_azure(blob_service_client, videos_metadata)

    except Exception as e:
        import traceback
        print("--- BACKGROUND PROCESS ERROR ---")
        traceback.print_exc() 
        print(f"Details: {e}")        
        
        videos_metadata = load_videos_from_azure(blob_service_client)
        if videos_metadata:
            for v in videos_metadata:
                if v['id'] == video_id:
                    v['status'] = 'failed'
                    break
            save_videos_to_azure(blob_service_client, videos_metadata)
    finally:
        # Cleanup temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

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

def load_videos_from_azure(blob_service_client, force_refresh=False):
    global video_cache
    
    # If we have a cache and don't need a refresh, return it instantly
    if video_cache is not None and not force_refresh:
        return video_cache

    if not blob_service_client:
        return []

    try:
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_METADATA_CONTAINER_NAME, blob=METADATA_BLOB_NAME
        )
        download_stream = blob_client.download_blob()
        data = json.loads(download_stream.readall().decode('utf-8'))
        
        # Update the global cache
        video_cache = data
        return video_cache
    
    except ResourceNotFoundError:
        video_cache = []
        return []
    except Exception as e:
        print(f"Error loading metadata: {e}")
        return video_cache if video_cache is not None else []
    

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


def generate_thumbnail(video_temp_path, thumb_temp_path, timestamp_sec):
    """
    Generates a thumbnail using a direct FFmpeg call to bypass 
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
    # print("this is request in get_videos", request.args)
    blob_service_client = get_blob_service_client()
    videos = load_videos_from_azure(blob_service_client)
    if videos is None: 
         return jsonify({"error": "Failed to load video metadata"}), 500

    # 1. CHECK FOR QUERY PARAMETER
    user_id_param = request.args.get('user_id')

    # 2. FILTER IF PARAM EXISTS
    if user_id_param:
        # Convert to string for comparison to be safe
        videos = [v for v in videos if str(v.get('user_id')) == str(user_id_param)]

    # 3. REVERSE ORDER (Newest first, like Instagram)
    videos.reverse()
    
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
    try:
        blob_service_client = get_blob_service_client()
        file = request.files['videoFile']
        
        # 1. Create Initial Metadata with 'processing' status
        videos_metadata = load_videos_from_azure(blob_service_client)
        next_id = max([v.get('id', 0) for v in videos_metadata]) + 1 if videos_metadata else 1
        
        raw_grade = request.form.get('grade', '0')
        try:
            grade_val = int(raw_grade)
        except ValueError:
            grade_val = 0

        new_video_entry = {
            "id": next_id,
            "title": request.form.get('title', 'Untitled'),
            "climbed_date": request.form.get('climbed_date'),
            "climb_type": request.form.get('climb_type'),
            "board_type": request.form.get('board_type'),
            "thumbnail": "https://placehold.co/600x400?text=Processing...", 
            "videoUrl": None,
            "grade": grade_val,
            "tags": [tag.strip() for tag in request.form.get('tags', '').split(',') if tag.strip()],
            "user_id": request.form.get('user_id'),
            "status": "processing" # <--- IMPORTANT
        }
        
        # Save placeholder to DB immediately
        videos_metadata.append(new_video_entry)
        save_videos_to_azure(blob_service_client, videos_metadata)

        # 2. Save file to a temp location for the background thread to use
        temp_dir = tempfile.mkdtemp(prefix='processing_')
        safe_filename = file.filename.replace(" ", "_")
        video_temp_path = os.path.join(temp_dir, safe_filename)
        file.save(video_temp_path)

        # 3. KICK OFF BACKGROUND THREAD
        # This returns control to the mobile app INSTANTLY
        thread = threading.Thread(
            target=background_video_processing, 
            args=(next_id, video_temp_path, safe_filename)
        )
        thread.start()

        return jsonify({
            "message": "Upload started! Video is being processed.",
            "video_id": next_id
        }), 202 # 202 means "Accepted for processing"

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

# updating video
@app.route('/api/videos/<int:video_id>', methods=['PUT'])
def update_video(video_id):
    data = request.get_json()
    blob_service_client = get_blob_service_client()
    videos = load_videos_from_azure(blob_service_client)
    
    # Find the video and update fields
    video = next((v for v in videos if v['id'] == video_id), None)
    if not video:
        return jsonify({"error": "Video not found"}), 404

    video['title'] = data.get('title', video['title'])
    video['tags'] = data.get('tags', video['tags'])

    if save_videos_to_azure(blob_service_client, videos):
        return jsonify({"message": "Updated successfully", "video": video})
    return jsonify({"error": "Failed to save changes"}), 500


# deleting video
@app.route('/api/videos/<int:video_id>', methods=['DELETE'])
def delete_video(video_id):
    blob_service_client = get_blob_service_client()
    videos = load_videos_from_azure(blob_service_client)
    
    video = next((v for v in videos if v['id'] == video_id), None)
    if not video:
        return jsonify({"error": "Video not found"}), 404

    try:
        def get_blob_name_from_url(url):
            if not url or not isinstance(url, str) or "placehold.co" in url: 
                return None
            return url.split('/')[-1]

        # Delete Video (if it exists)
        v_name = get_blob_name_from_url(video.get('videoUrl'))
        if v_name:
            blob_service_client.get_blob_client(AZURE_VIDEO_FILES_CONTAINER_NAME, v_name).delete_blob()

        # Delete Thumbnail (if it exists)
        t_name = get_blob_name_from_url(video.get('thumbnail'))
        if t_name:
            blob_service_client.get_blob_client(AZURE_THUMBNAILS_CONTAINER_NAME, t_name).delete_blob()
            
    except Exception as e:
        print(f"Cleanup skipped or failed: {e}")

    # Remove from metadata
    new_videos = [v for v in videos if v['id'] != video_id]
    
    if save_videos_to_azure(blob_service_client, new_videos):
        # IMPORTANT: Force a cache refresh so the UI sees the change immediately
        load_videos_from_azure(blob_service_client, force_refresh=True)
        return jsonify({"message": "Deleted successfully"})
    return jsonify({"error": "Failed to delete"}), 500


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
