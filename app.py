# app.py
import os
import threading 
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

AZURE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
AZURE_VIDEO_FILES_CONTAINER_NAME = os.environ.get('AZURE_VIDEO_FILES_CONTAINER_NAME', 'climbing-journal-videos') 
AZURE_THUMBNAILS_CONTAINER_NAME = os.environ.get('AZURE_THUMBNAILS_CONTAINER_NAME', 'climbing-journal-thumbnails')


# Load Azure PostgreSQL connection string from environment variable
DB_CONFIG = {
    "host": os.environ.get('SECRET_PGHOST'),
    "user": os.environ.get('SECRET_PGUSER'),
    "password": os.environ.get('SECRET_PGPASSWORD'), 
    "dbname": os.environ.get('SECRET_PGDATABASE'),
    "port": int(os.environ.get('SECRET_PGPORT',5432)),
}


# Thumbnail generation settings
THUMBNAIL_TIME_SECONDS = 5.0 # Time in seconds to grab the frame
THUMBNAIL_FILENAME_SUFFIX = '_thumb.jpg' # Suffix for thumbnail files
THUMBNAIL_CONTENT_TYPE = 'image/jpeg'

VIDEO_PROCESSING_TIMEOUT = 600  # 600 seconds (10 minutes)

# Determine the absolute path to the directory this script is in
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# Define the path to the static frontend files (assuming they are in a 'frontend' subdirectory)
STATIC_FOLDER = os.path.join(BASE_DIR, 'frontend')
FFMPEG_BINARY = os.path.join(BASE_DIR, 'bin', 'ffmpeg')


app = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='')

def ensure_ffmpeg_permissions():
    if os.path.exists(FFMPEG_BINARY):
        try:
            print(f"🔧 Setting executable permissions for: {FFMPEG_BINARY}", flush=True)
            os.chmod(FFMPEG_BINARY, 0o755)
        except Exception as e:
            print(f"❌ Could not set permissions: {e}", flush=True)

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG, sslmode='require')
        return conn
    except Exception as e:
        print(f"Database connection error: {e}", flush=True)
        return None



try: 
    from flask_cors import CORS
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings, __version__ as azure_storage_version
    from azure.core.exceptions import ResourceNotFoundError

    # --- CORS Configuration ---
    CORS(app) # Keep CORS enabled
except:
    pass


import psycopg2
from psycopg2.extras import RealDictCursor



def init_db():
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    # CREATE videos metadata table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.videos (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            climbed_date DATE,
            grade INTEGER,
            climb_type TEXT,
            board_type TEXT,
            climb_url TEXT,
            board_angle INTEGER,
            description TEXT,
            thumbnail TEXT,
            video_url TEXT,
            send BOOLEAN DEFAULT FALSE,
            tags TEXT[],
            user_id TEXT,
            user_name TEXT,
            status TEXT DEFAULT 'processing'
        );
    ''')

    # CREATE comments table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.comments (
            id SERIAL PRIMARY KEY,
            video_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            comment_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_video
                FOREIGN KEY(video_id) 
                REFERENCES videos(id)
                ON DELETE CASCADE
        );
    ''')

    conn.commit()
    cur.close()
    conn.close()

    return

# Run initialization
ensure_ffmpeg_permissions()
init_db()


# --- Core Logic --- 

def background_video_processing(video_id, input_temp_path, safe_filename):
    temp_dir = os.path.dirname(input_temp_path)
    
    try:
        print(f"--- Starting background processing for Video ID: {video_id} ---", flush=True)
        blob_service_client = get_blob_service_client()
        base_name = os.path.splitext(safe_filename)[0]
        
        processed_video_path = os.path.join(temp_dir, f"{base_name}_1080p.mp4")
        thumb_temp_path = os.path.join(temp_dir, f"{base_name}_thumb.jpg")

        # 1. FFmpeg: SCALE AND RE-ENCODE WITH TIMEOUT
        print(f"[{video_id}] Running FFmpeg encoding (Timeout: {VIDEO_PROCESSING_TIMEOUT}s)...", flush=True)
        encode_cmd = [
            FFMPEG_BINARY, '-y', '-i', input_temp_path,
            '-vf', "scale='min(1920,iw)':-2", 
            '-vcodec', 'libx264', '-crf', '23', '-preset', 'ultrafast',
            '-c:a', 'copy', '-movflags', 'faststart', '-threads', '0', 
            processed_video_path
        ]
        
        try:
            # Add the timeout parameter here
            result = subprocess.run(
                encode_cmd, 
                capture_output=True, 
                text=True, 
                timeout=VIDEO_PROCESSING_TIMEOUT
            )
            
            if result.returncode != 0:
                raise Exception(f"FFmpeg encoding failed: {result.stderr}")

        except subprocess.TimeoutExpired:
            # Handle the specific case where FFmpeg takes too long
            raise Exception(f"FFmpeg processing timed out after {VIDEO_PROCESSING_TIMEOUT} seconds.")
        
        # 2. GENERATE THUMBNAIL
        print(f"[{video_id}] Generating thumbnail...", flush=True)
        thumb_success = generate_thumbnail(processed_video_path, thumb_temp_path, 1.0)
        if not thumb_success:
            print(f"[{video_id}] Warning: Thumbnail generation failed, using placeholder.", flush=True)

        # 3. UPLOAD TO AZURE
        # Upload Video
        video_blob_name = f"{uuid.uuid4()}.mp4"
        print(f"[{video_id}] Uploading video to Azure...", flush=True)
        with open(processed_video_path, 'rb') as v_file:
            video_url = upload_blob_to_azure(
                blob_service_client, AZURE_VIDEO_FILES_CONTAINER_NAME,
                video_blob_name, v_file, 'video/mp4'
            )

        # Upload Thumbnail (if generated)
        thumb_url = "https://placehold.co/600x400?text=No+Thumb"
        if os.path.exists(thumb_temp_path):
            print(f"[{video_id}] Uploading thumbnail to Azure...", flush=True)
            thumb_blob_name = f"{uuid.uuid4()}.jpg"
            with open(thumb_temp_path, 'rb') as t_file:
                uploaded_thumb_url = upload_blob_to_azure(
                    blob_service_client, AZURE_THUMBNAILS_CONTAINER_NAME,
                    thumb_blob_name, t_file, 'image/jpeg'
                )
                if uploaded_thumb_url:
                    thumb_url = uploaded_thumb_url

        # 4. UPDATE POSTGRESQL STATUS
        print(f"[{video_id}] Updating database status to 'completed'...", flush=True)
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            UPDATE videos 
            SET video_url = %s, thumbnail = %s, status = 'completed'
            WHERE id = %s
        ''', (video_url, thumb_url, video_id))
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"--- Successfully processed Video ID: {video_id} ---", flush=True)

    except Exception as e:
        print(f"!!! FATAL ERROR for Video ID {video_id}: {str(e)} !!!", flush=True)
        
        # Update DB to 'failed' so user knows it's stuck
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("UPDATE videos SET status = 'failed' WHERE id = %s", (video_id,))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as db_err:
            print(f"Could not update status to failed: {db_err}", flush=True)

    finally:
        # 5. CLEANUP TEMP FILES
        if os.path.exists(temp_dir):
            print(f"[{video_id}] Cleaning up temp directory: {temp_dir}", flush=True)
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


def get_videos_from_db(user_id=None):
    """
    Fetches videos from PostgreSQL. 
    If user_id is provided, it filters for that user.
    """
    conn = get_db_connection()
    if not conn:
        return []
        
    try:
        # RealDictCursor makes the result look like your old JSON (a list of dictionaries)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if user_id:
            # Filtered view (Profile)
            query = "SELECT * FROM videos WHERE user_id = %s ORDER BY id DESC"
            cur.execute(query, (str(user_id),))
        else:
            # Global view (Feed)
            query = "SELECT * FROM videos ORDER BY id DESC"
            cur.execute(query)
            
        videos = cur.fetchall()
        cur.close()
        
        # PostgreSQL returns 'datetime' objects for dates, 
        # but JSON needs strings. We convert them here.
        for v in videos:
            if v['climbed_date']:
                v['climbed_date'] = v['climbed_date'].isoformat()
                
        return videos
    except Exception as e:
        print(f"Error fetching from DB: {e}", flush=True)
        return []
    finally:
        conn.close()


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
            FFMPEG_BINARY,
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


# --- Video API Routes ---

@app.route('/api/videos', methods=['GET'])
def get_videos():
    user_id_param = request.args.get('user_id')
    
    # Use the new DB function instead of the old JSON loader
    videos = get_videos_from_db(user_id_param)
    
    if videos is None: 
         return jsonify({"error": "Failed to load videos"}), 500

    # Note: We don't need .reverse() anymore because 
    # the SQL query uses "ORDER BY id DESC"
    return jsonify(videos)


@app.route('/api/tags', methods=['GET'])
def get_tags():
    conn = get_db_connection()
    if not conn: return jsonify([])
    
    try:
        cur = conn.cursor()
        # This special Postgres syntax expands the tags array and finds unique values
        cur.execute("SELECT DISTINCT unnest(tags) FROM videos WHERE tags IS NOT NULL ORDER BY 1")
        tags = [row[0] for row in cur.fetchall()]
        cur.close()
        return jsonify(tags)
    except Exception as e:
        print(f"Error fetching tags: {e}")
        return jsonify([])
    finally:
        conn.close()


# uploading video
@app.route('/api/upload', methods=['POST'])
def upload_video():
    try:
        # 1. Collect form data
        title = request.form.get('title', 'Untitled')
        climbed_date = request.form.get('climbed_date')
        climb_type = request.form.get('climb_type')
        board_type = request.form.get('board_type')

        raw_angle = request.form.get('board_angle')
        board_angle = int(raw_angle) if raw_angle and raw_angle.strip() != '' else None

        user_id = request.form.get('user_id')
        user_name = request.form.get('user_name') 
        description = request.form.get('description', '').strip()
        climb_url = request.form.get('climb_url', '')

        send_raw = request.form.get('send', 'false').lower()
        is_send = True if send_raw == 'true' else False

        # Handle Grade safely
        try:
            grade = int(request.form.get('grade', 0))
        except (ValueError, TypeError):
            grade = 0
            
        # Handle Tags (Convert comma-string to Python list)
        tags_raw = request.form.get('tags', '')
        tags_list = [t.strip() for t in tags_raw.split(',') if t.strip()]

        # 2. Insert into PostgreSQL with 'processing' status
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO videos (
                    title, 
                    climbed_date, 
                    climb_type, 
                    board_type,
                    board_angle,
                    grade, 
                    tags, 
                    user_id, 
                    user_name, 
                    status, 
                    thumbnail, 
                    send, 
                    description, 
                    climb_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            title, climbed_date, climb_type, board_type, board_angle, grade, tags_list, 
            user_id, user_name, 'processing', "https://placehold.co/600x400?text=Processing...", is_send, description, climb_url
        ))
        
        new_video_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        # 3. Handle the File
        file = request.files['videoFile']
        temp_dir = tempfile.mkdtemp(prefix='processing_')
        safe_filename = file.filename.replace(" ", "_")
        video_temp_path = os.path.join(temp_dir, safe_filename)
        file.save(video_temp_path)

        # 4. Start Background Thread
        thread = threading.Thread(
            target=background_video_processing, 
            args=(new_video_id, video_temp_path, safe_filename)
        )
        thread.start()

        return jsonify({
            "message": "Upload started! Video is being processed.",
            "video_id": new_video_id
        }), 202

    except Exception as e:
        print(f"Upload error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500
        

# updating video
@app.route('/api/videos/<int:video_id>', methods=['PUT'])
def update_video(video_id):
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Extract fields from request
    title = data.get('title')
    # If tags come in as a string, split them; if they are already a list, use as is.
    tags = data.get('tags')
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(',') if t.strip()]

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Update the record in PostgreSQL
        # We use COALESCE to keep the old value if the new one isn't provided
        cur.execute('''
            UPDATE videos 
            SET title = COALESCE(%s, title), 
                climbed_date = %s,
                tags = COALESCE(%s, tags),
                grade = COALESCE(%s, grade),
                send = COALESCE(%s, send),
                climb_type = COALESCE(%s, climb_type),
                board_type = COALESCE(%s, board_type),
                board_angle = %s,
                description = COALESCE(%s, description),
                climb_url = COALESCE(%s, climb_url)
            WHERE id = %s
            RETURNING *
        ''', (
            title, 
            data.get('climbed_date'),
            tags, 
            data.get('grade'), 
            data.get('send'), 
            data.get('climb_type'), 
            data.get('board_type'), 
            data.get('board_angle'),
            data.get('description'),
            data.get('climb_url'),
            video_id
        ))        
        updated_video = cur.fetchone()
        conn.commit()
        
        if not updated_video:
            return jsonify({"error": "Video not found"}), 404

        print(f"Successfully updated Video ID: {video_id}", flush=True)
        return jsonify({
            "message": "Updated successfully", 
            "video": updated_video
        })

    except Exception as e:
        print(f"Error updating video {video_id}: {e}", flush=True)
        return jsonify({"error": "Failed to save changes"}), 500
    finally:
        cur.close()
        conn.close()

# deleting video
@app.route('/api/videos/<int:video_id>', methods=['DELETE'])
def delete_video(video_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 1. Get info to delete blobs
    cur.execute("SELECT video_url, thumbnail FROM videos WHERE id = %s", (video_id,))
    video = cur.fetchone()
    
    if not video:
        return jsonify({"error": "Not found"}), 404

    # 2. Azure Cleanup Logic
    try:
        blob_service_client = get_blob_service_client()
        for key in ['video_url', 'thumbnail']:
            url = video.get(key)
            if url and "placehold.co" not in url:
                blob_name = url.split('/')[-1]
                container = AZURE_VIDEO_FILES_CONTAINER_NAME if key == 'video_url' else AZURE_THUMBNAILS_CONTAINER_NAME
                blob_service_client.get_blob_client(container, blob_name).delete_blob()
    except Exception as e:
        print(f"Blob deletion error: {e}")

    # 3. DB Cleanup
    cur.execute("DELETE FROM videos WHERE id = %s", (video_id,))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "Deleted successfully"})


# --- Comment Routes ---

@app.route('/api/videos/<int:video_id>/comments', methods=['GET'])
def get_comments(video_id):
    conn = get_db_connection()
    if not conn: return jsonify([])
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Get comments, newest first
        cur.execute("""
            SELECT * FROM comments 
            WHERE video_id = %s 
            ORDER BY created_at DESC
        """, (video_id,))
        comments = cur.fetchall()
        
        # Format dates to string
        for c in comments:
            if c['created_at']:
                c['created_at'] = c['created_at'].strftime('%Y-%m-%d %H:%M')
                
        return jsonify(comments)
    except Exception as e:
        print(f"Error getting comments: {e}")
        return jsonify([])
    finally:
        cur.close()
        conn.close()

@app.route('/api/videos/<int:video_id>/comments', methods=['POST'])
def add_comment(video_id):
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({"error": "Comment text required"}), 400

    user_id = data.get('user_id')
    user_name = data.get('user_name', 'Anonymous')
    text = data.get('text')

    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            INSERT INTO comments (video_id, user_id, user_name, comment_text)
            VALUES (%s, %s, %s, %s)
            RETURNING id, created_at
        """, (video_id, user_id, user_name, text))
        
        result = cur.fetchone()
        conn.commit()
        
        # Return the new comment data so frontend can update immediately
        return jsonify({
            "id": result['id'],
            "video_id": video_id,
            "user_id": user_id,
            "user_name": user_name,
            "comment_text": text,
            "created_at": result['created_at'].strftime('%Y-%m-%d %H:%M')
        })
    except Exception as e:
        print(f"Error adding comment: {e}")
        return jsonify({"error": "Failed to add comment"}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/comments/<int:comment_id>', methods=['PUT'])
def update_comment(comment_id):
    data = request.get_json()
    new_text = data.get('text')
    request_user_id = data.get('user_id') # Passed from frontend to verify ownership

    if not new_text or not request_user_id:
        return jsonify({"error": "Missing data"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Verify ownership
        cur.execute("SELECT user_id FROM comments WHERE id = %s", (comment_id,))
        comment = cur.fetchone()
        
        if not comment:
            return jsonify({"error": "Comment not found"}), 404
            
        if str(comment['user_id']) != str(request_user_id):
            return jsonify({"error": "Unauthorized"}), 403

        # 2. Update
        cur.execute("""
            UPDATE comments 
            SET comment_text = %s 
            WHERE id = %s 
            RETURNING *
        """, (new_text, comment_id))
        
        updated_comment = cur.fetchone()
        conn.commit()
        
        # Format date for consistency
        if updated_comment['created_at']:
            updated_comment['created_at'] = updated_comment['created_at'].strftime('%Y-%m-%d %H:%M')

        return jsonify(updated_comment)

    except Exception as e:
        print(f"Error updating comment: {e}")
        return jsonify({"error": "Update failed"}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/api/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    data = request.get_json()
    request_user_id = data.get('user_id')

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 1. Verify ownership
        cur.execute("SELECT user_id FROM comments WHERE id = %s", (comment_id,))
        comment = cur.fetchone() # returns tuple in standard cursor, or dict in RealDict
        
        # Handle tuple vs dict depending on cursor factory (standard cursor returns tuple)
        # To be safe, let's fetch strictly as tuple for this check
        if not comment:
            return jsonify({"error": "Comment not found"}), 404

        # Standard cursor returns tuple (user_id,)
        # RealDictCursor returns {'user_id': ...}
        # Since we initialized RealDictCursor in previous snippets, let's assume dict access:
        # BUT strictly speaking, get_db_connection returns a raw connection. 
        # Let's write query to handle check safely.
        
        db_user_id = comment[0] if isinstance(comment, tuple) else comment['user_id']

        if str(db_user_id) != str(request_user_id):
             return jsonify({"error": "Unauthorized"}), 403

        # 2. Delete
        cur.execute("DELETE FROM comments WHERE id = %s", (comment_id,))
        conn.commit()
        
        return jsonify({"message": "Deleted successfully"})

    except Exception as e:
        print(f"Error deleting comment: {e}")
        return jsonify({"error": "Delete failed"}), 500
    finally:
        cur.close()
        conn.close()


# --- Route to serve the frontend (obsolete not used) ---
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


@app.route('/api/debug', methods=['GET'])
def debug_ffmpeg():
    results = {
        "binary_path": FFMPEG_BINARY,
        "exists": os.path.exists(FFMPEG_BINARY),
        "is_executable": os.access(FFMPEG_BINARY, os.X_OK) if os.path.exists(FFMPEG_BINARY) else False,
        "ffmpeg_version_output": None,
        "error": None
    }
    
    if results["exists"]:
        try:
            # Try to run 'ffmpeg -version'
            res = subprocess.run([FFMPEG_BINARY, '-version'], capture_output=True, text=True, timeout=5)
            results["ffmpeg_version_output"] = res.stdout.split('\n')[0] # Get first line
        except Exception as e:
            results["error"] = str(e)
    
    return jsonify(results)