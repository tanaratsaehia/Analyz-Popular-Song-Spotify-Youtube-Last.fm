import math
from fastapi import FastAPI, Query
from pymongo import MongoClient
from bson.objectid import ObjectId

# MongoDB connection details
USER_NAME = 'oil'
PASSWORD = 'dataEn1122344'
DATABASE_IP = 'localhost'
MONGO_URI = f"mongodb://{USER_NAME}:{PASSWORD}@{DATABASE_IP}:27017/DataEngineer"
DATABASE_NAME = "DataEngineer"
TRACKS_COLLECTION = "Tracks"

# Initialize FastAPI app
app = FastAPI()

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
tracks_collection = db[TRACKS_COLLECTION]

def clean_data(data):
    """ Convert NaN, Infinity, -Infinity to None for JSON compatibility. """
    if isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
        return None
    return data

@app.get("/songs")
def get_songs(limit: int = Query(None, description="Number of songs to return (default: all)")):
    """
    Fetches song information with optional limit.
    If limit is not provided, all records will be returned.
    Example:
      - /songs -> returns all records
      - /songs?limit=100 -> returns 100 records
    """
    pipeline = [
        {
            "$lookup": {
                "from": "Albums",
                "localField": "album_id",
                "foreignField": "_id",
                "as": "album_info"
            }
        },
        {
            "$unwind": {"path": "$album_info", "preserveNullAndEmptyArrays": True}
        },
        {
            "$lookup": {
                "from": "Artists",
                "localField": "artist_id",
                "foreignField": "_id",
                "as": "artist_info"
            }
        },
        {
            "$lookup": {
                "from": "Release",
                "localField": "release_id",
                "foreignField": "_id",
                "as": "release_info"
            }
        },
        {
            "$unwind": {"path": "$release_info", "preserveNullAndEmptyArrays": True}
        },
        {
            "$project": {
                "_id": 0,
                "track_name": 1,
                "album": "$album_info.name",
                "artists": {
                    "$cond": {
                        "if": {"$isArray": "$artist_info.name"},
                        "then": "$artist_info.name",
                        "else": ["Unknown Artist"]
                    }
                },
                "release_year": "$release_info.year",
                "release_month": "$release_info.month",
                "release_day": "$release_info.day",
                "popularity": 1,
                "view_count": 1,
                "like_count": 1,
                "listeners": 1,
                "play_count": 1
            }
        }
    ]

    # Apply limit only if provided
    if limit is not None:
        pipeline.append({"$limit": limit})

    result = list(tracks_collection.aggregate(pipeline))

    # Convert NaN, Infinity, -Infinity values to None
    for song in result:
        for key, value in song.items():
            song[key] = clean_data(value)

    return {"total_songs": len(result), "songs": result}

# Run FastAPI server
# Command: uvicorn main:app --host 0.0.0.0 --port 8854
