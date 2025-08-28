from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "library_db")
COLLECTION = os.getenv("COLLECTION", "books")

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION]
