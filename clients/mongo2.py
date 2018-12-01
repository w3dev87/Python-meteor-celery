import pymongo
import ssl
import os

mongo_db = pymongo.MongoClient(os.environ["MONGO_URL"], ssl_cert_reqs=ssl.CERT_NONE, connect=False).get_default_database()


def get_collection(collection_name):
    return mongo_db[collection_name]
