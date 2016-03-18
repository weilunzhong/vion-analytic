import csv
from pymongo import MongoClient
from elasticsearch import Elasticsearch

es = Elasticsearch()

MONGO_HOST = "192.168.1.87"
MONGO_PORT = 27017
mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
db = mongo_client.userdata
collection = db.cmore_2015_11

def parse_time(time_string):
    time_list = time_string.split(':')
    return 3600*int(time_list[0]) + 60*int(time_list[1]) + int(time_list[2])

def validate_document(usr_document):
    imdbid = usr_document["IMDb ID"]
    playtime = usr_document["Playtime"]
    if (imdbid != "NULL") and (playtime>5):
        return True
    else:
        return False

def populate_database(source_file):
    with open(source_file, "rb") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
        for idx, line in enumerate(spamreader):
            if idx == 0:
                dict_keys = line
                dict_keys[0] = "Stream ID"
            else:
                if len(line) != 18:
                    raise RuntimeError("csv file has empty field and is not parsed properly")
                res = {dict_keys[i] :line[i] for i in range(0,18)}
                res["Playtime"] = parse_time(res["Playtime"])
                validation = validate_document(res)
                if validation:
                    collection.insert(res)
                else:
                    continue
                if idx % 1000 == 5:
                    print collection.count()

def populate_elastic(source_file):
    with open(source_file, "rb") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
        for idx, line in enumerate(spamreader):
            if idx == 0:
                dict_keys = line
                dict_keys[0] = "Stream ID"
            else:
                if len(line) != 18:
                    raise RuntimeError("csv file has empty field and is not parsed properly")
                res = {dict_keys[i] :line[i] for i in range(0,18)}
                res["Playtime"] = parse_time(res["Playtime"])
                validation = validate_document(res)
                if validation:
                    indexer = es.index(index="user_data", doc_type="cmore",
                                   id = res["Stream ID"], body=res)
                else:
                    continue
            print idx

def flush_db_collection(db_collection):
    db_collection.remove({})
    print db_collection.count()

if __name__ == "__main__":
    populate_elastic("usr_data/streamcounts_20150101-20150201.csv")
    # flush_db_collection(collection)
