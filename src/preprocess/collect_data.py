import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient, database

load_dotenv('../env/mongo.env')

def connect() -> MongoClient:
    client = MongoClient(
        host=os.getenv("HOST"),
        username=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD"),
        authSource=os.getenv("AUTHSOURCE"),
        authMechanism=os.getenv("AUTHMECHANISM")
    )

    return client

def read_raw_data( f_path : str , db : database.Database ) -> None:
    with open( f_path ) as f:
        data = json.load( f )
        db.get_collection(os.path.basename( f_path )).insert_many( data )

if __name__ == '__main__':
    dirPath = "../../rev-rec-data/"

    conn = connect()
    for f_name in os.listdir( dirPath ):
        read_raw_data( os.path.join( dirPath, f_name ), conn.CRR )
    conn.close()
    
