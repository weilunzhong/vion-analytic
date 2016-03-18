from pymongo import MongoClient
import json

MONGO_HOST = "192.168.1.87"
MONGO_PORT = 27017
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client.userdata
collection = db.cmore_2015

def find_document(key, value):
    res = collection.find({key, value})

def usr_aggregation():
    usr_list = collection.distinct("Member ID")

def operator_aggregation():
    operator_list = collection.distinct("Operator")

def time_aggregation():
    pass

def imdb_aggregation():
    imdbID_list = collection.distinct("IMDB ID")

def device_aggregation():
    platform_list = collection.distinct("Device type")

def main():
    pass

if __name__ == "__main__":
    main()
