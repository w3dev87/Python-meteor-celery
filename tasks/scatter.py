"""
scatter.py is a one off script inteded to be run as a subprocess. It is a 'worker-process' called multiple times within
scatter_broadcast.py. It is experimental and is not, and should not be used in production.
"""

import os
import sys

# hack to allow us to import from sibling directory
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients.logger import Logger
from clients.mongo import Mongo
from clients.postgres import Postgres
from clients._redis import Redis
from clients.copilot import Copilot


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

    # configuring redis
    redis = Redis(os.environ["REDIS_HOST"], os.environ["REDIS_PORT"], os.environ["REDIS_PASSWORD"])

    # configure our Copilot client
    copilot = Copilot(os.environ["TWILIO_ACCOUNT_ID"], os.environ["TWILIO_AUTH_TOKEN"], redis.get_client())


except Exception as e:
    log.error(e)
    sys.exit(0)


def action():
    if len(sys.argv) < 10:
        log.error(("In sufficient arguments: " + str(len(sys.argv))))
        sys.exit(-1)

    group_id, message, locale, media_url, tracker_id, broadcast_id, user_id, chunk, chunk_size = sys.argv[1:]

    group = Group.select().where(Group.id == group_id).get()
    contacts = Contact.select().where(Contact.group_id == group_id).paginate(int(chunk), int(chunk_size))

    index = 0
    successes = 0
    failures = 0
    for contact in contacts:
        if index % 100 == 0:
            tracker_collection.update_one({"_id": tracker_id}, {"$inc": {"progress": successes, "failures": failures}})
            successes = 0
            failures = 0

        try:
            if group.reuse:
                sid = copilot.send(contact.phone, message, locale, media_url, contact.sid)
                copilot.set_key(contact.phone, sid, broadcast_id, group_id, user_id, media_url, tracker_id=tracker_id)
            else:
                sid = copilot.send(contact.phone, message, locale, media_url)
                copilot.set_key(contact.phone, sid, broadcast_id, group_id, user_id, media_url)
                copilot.add_service(contact.phone, sid)

            successes += 1

        except Exception as e:
            log.info("Failed to broadcast for contact: {0}".format(contact.phone))
            log.error(e)
            failures += 1

        index += 1

    tracker_collection.update_one({"_id": tracker_id}, {"$inc": {"successes": successes}})

    log.info("Successful broadcast to group_id = {0}, contacts = {1} of chunk {1}".format(group_id, index,
                                                                                          chunk))
    sys.exit(1)


if __name__ == "__main__":
    action()
