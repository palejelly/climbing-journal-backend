import json
import psycopg2
from app import get_blob_service_client, get_db_connection

def migrate_json_to_postgres():
    # 1. Download JSON from Azure
    print("Downloading videos.json from Azure...")
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container='climbing-journal-storage', blob='videos.json'
    )
    
    try:
        data = blob_client.download_blob().readall()
        videos = json.loads(data)
    except Exception as e:
        print(f"Could not find or read JSON: {e}")
        return

    # 2. Connect to Postgres
    conn = get_db_connection()
    cur = conn.cursor()

    print(f"Found {len(videos)} videos. Starting migration...")

    for v in videos:
        # Ensure tags is a list
        tags = v.get('tags', [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(',') if t.strip()]

        try:
            cur.execute('''
                INSERT INTO videos (id, title, climbed_date, grade, climb_type, board_type, thumbnail, video_url, tags, user_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            ''', (
                v.get('id'),
                v.get('title', 'Untitled'),
                v.get('climbed_date'),
                v.get('grade', 0),
                v.get('climb_type'),
                v.get('board_type'),
                v.get('thumbnail'),
                v.get('videoUrl'), # Mapping JSON 'videoUrl' to DB 'video_url'
                tags,
                v.get('user_id'),
                v.get('status', 'completed')
            ))
        except Exception as e:
            print(f"Failed to migrate video {v.get('id')}: {e}")
            conn.rollback()
            continue

    conn.commit()
    # Reset the ID sequence so new uploads don't conflict with migrated IDs
    cur.execute("SELECT setval('videos_id_seq', (SELECT MAX(id) FROM videos));")
    conn.commit()
    
    cur.close()
    conn.close()
    print("Migration complete!")

if __name__ == "__main__":
    migrate_json_to_postgres()