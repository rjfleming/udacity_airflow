class SqlQueries:
    SONGPLAY_TABLE_INSERT = ("""
        INSERT INTO songplays (id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    USER_TABLE_INSERT = ("""
        INSERT INTO users (id, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    SONG_TABLE_INSERT = ("""
        INSERT into songs (id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    ARTIST_TABLE_INSERT = ("""
        INSERT INTO artists (id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    TIME_TABLE_INSERT = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, 
               extract(hour from start_time),
               extract(day from start_time),
               extract(week from start_time), 
               extract(month from start_time),
               extract(year from start_time),
               extract(dayofweek from start_time)
        FROM songplays
    """)
    
    S3_REDSHIFT_COPY_SQL = ("""
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                json '{}'
                """)
    
    TRUNCATE_TABLE = ("TRUNCATE {}")