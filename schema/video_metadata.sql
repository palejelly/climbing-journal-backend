-- Schema for Climbing Journal Video App
-- Database: PostgreSQL

CREATE TABLE IF NOT EXISTS videos (
    -- SERIAL handles auto-incrementing IDs
    id SERIAL PRIMARY KEY,
    
    -- Metadata
    title TEXT NOT NULL,
    climbed_date DATE,
    grade INTEGER DEFAULT 0,
    climb_type TEXT, -- e.g., 'board', 'gym', 'outdoor'
    board_type TEXT, -- e.g., 'Kilter', 'Tension 2'
    
    -- File Links
    thumbnail TEXT,
    video_url TEXT,
    
    -- Postgres Arrays are great for tags
    tags TEXT[] DEFAULT '{}',
    
    -- Auth/User Tracking
    user_id TEXT,
    
    -- Processing State
    status TEXT DEFAULT 'processing',
    
    -- Timestamps for better sorting later
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexing for speed: 
-- This makes searching by user_id or filtering by tags instant.
CREATE INDEX idx_videos_user_id ON videos(user_id);
CREATE INDEX idx_videos_tags ON videos USING GIN (tags);