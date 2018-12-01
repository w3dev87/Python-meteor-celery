import pymongo
import ssl


# Wrapper for using the pymongo ORM with a MongoDB database
class Mongo:
    def __init__(self, url):
        self.client = pymongo.MongoClient(url, ssl_cert_reqs=ssl.CERT_NONE)
        self.db = self.client.get_default_database()

    def get_collection(self, collection_name):
        return self.db[collection_name]
