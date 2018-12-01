"""
scatter_broadcast.py is a one-off script intended to be run as a sub-process. It breaks a broadcast into multiple 'chunks' which
are then executed in their own sub-process derived from scatter.py. It was experimental and was deprecated in favor of
celery tasks. As such I won't go into depth on it. It is dead-code.
"""

import datetime
import os
import sys
import math
import requests
from bson.objectid import ObjectId

# hack to allow us to import from sibling directory
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients.logger import Logger
from clients.mongo import Mongo
from clients.postgres import Postgres

CHUNK_SIZE = 1000
REQUEST_ENDPOINT = os.environ["SCATTER_API_ENDPOINT"]

# configure logging
try:
    logger = Logger(os.environ["LOGGLY_TOKEN"])
    log = logger.get_logger()
except Exception as e:
    print(e)
    sys.exit(0)

# configure our databases
try:
    # configuring postgres
    postgres = Postgres(os.environ["POSTGRES_DB"], os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"],
                        os.environ["POSTGRES_HOST"], os.environ["POSTGRES_PORT"])
    postgres_db = postgres.get_db()
    Contact = postgres.get_model("Contact")
    Group = postgres.get_model("Group")

    # configuring mongodb
    mongo_db = Mongo(os.environ["MONGO_URL"])
    broadcast_collection = mongo_db.get_collection("broadcasts")
    tracker_collection = mongo_db.get_collection("trackers")

except Exception as e:
    log.error(e)
    sys.exit(0)


def action():
    if len(sys.argv) < 7:
        log.error(("In sufficient arguments: " + str(len(sys.argv))))
        sys.exit(-1)

    group_id, message, locale, user_id, media_url, tracker_id = sys.argv[1:]
    group = Group.select().where(Group.id == group_id).get()

    try:
        contact_count = Contact.select().where(Contact.group_id == group_id).count()
        num_chunks = math.ceil(contact_count / CHUNK_SIZE)
        log.info("Broadcasting to {0} contacts with {1} chunks.".format(contact_count, num_chunks))
    except Exception as e:
        log.info("Failed to query group.")
        log.error(e)
        sys.exit(-1)

    if group.reuse:
        broadcast_id = group.broadcast_id
        broadcast_collection.update_one({"_id": ObjectId(group.broadcast_id)},
            {
                "$push": {
                    "broadcasts": {
                        "groupName": group.name,
                        "trackerId": tracker_id,
                        "groupId": group_id,
                        "userIds": [user_id],
                        "createdAt": datetime.datetime.now(),
                        "recipientsCount": contact_count,
                        "message": message,
                        "media_url": media_url,
                        "reuse": group.reuse
                    }
                }
            }
        )
    else:
        # needs a try-except here
        broadcast = broadcast_collection.insert_one({
            "groupName": group.name,
            "groupId": group_id,
            "userIds": [user_id],
            "createdAt": datetime.datetime.now(),
            "recipientsCount": contact_count,
            "message": message,
            "media_url": media_url,
            "reuse": group.reuse
        })
        broadcast_id = broadcast.inserted_id

    log.info("broadcast_id: {0}".format(broadcast_id))

    successes = 0
    for i in range(num_chunks):
        # message, locale, media_url, broadcast_id, group.id, user_id, chunk, chunk_size
        # make api request
        data = {
            "token": os.environ["PYTHON_APP_KEY"],
            "groupId": group_id,
            "message": message,
            "locale": locale,
            "mediaUrl": media_url,
            "trackerId": tracker_id,
            "broadcastId": str(broadcast_id),
            "chunk": str(i + 1),
            "chunkSize": str(CHUNK_SIZE),
            "userId": user_id
        }

        r = requests.post(REQUEST_ENDPOINT, json=data)

        if r.status_code == 200:
            successes += 1
            continue

        log.error("Failed to send request.")
        log.info(r)

    tracker_collection.update_one({"_id": tracker_id}, {"$set": {"processes": successes}})

    log.info("Successful broadcast to group_id = {0}, contacts = {1}".format(group_id, contact_count))
    sys.exit(1)


if __name__ == "__main__":
    action()
