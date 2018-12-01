"""
broadcast.py is intended to be run within a subprocess as a one off script. It executes a range of statements
corresponding to a broadcast's behavior, including various points of persistance and API calls to Twilio. Note this is
not how our application currently does broadcasts, but is a remnant of earlier experiments I did while creating this
feature.

To see how we actually do broadcasts check the celery-app repository.

Of note: the Try-Except blocks attempt to provide some error handling. Though this error handling is extremely
sub-optimal and not the least bit comprehensive. Production code should always implement better error handling then
this.
"""

import datetime
import os
import sys

# hack to allow us to import from sibling directory
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients._redis import Redis
from clients.copilot import Copilot
from clients.logger import Logger
from clients.mongo import Mongo
from clients.postgres import Postgres

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

    # configuring redis
    redis = Redis(os.environ["REDIS_HOST"], os.environ["REDIS_PORT"], os.environ["REDIS_PASSWORD"])

    # configure our Copilot client
    copilot = Copilot(os.environ["TWILIO_ACCOUNT_ID"], os.environ["TWILIO_AUTH_TOKEN"], redis.get_client())

except Exception as e:
    log.error(e)
    sys.exit(0)


def action():
    if len(sys.argv) < 6:
        log.error(("In sufficient arguments: " + str(len(sys.argv))))
        sys.exit(-1)

    group_id, message, locale, user_id, media_url = sys.argv[1:]

    try:
        group = Group.get(Group.id == group_id)
        contact_count = Contact.select().where(Contact.group_id == group_id).count()
        log.info("Broadcasting to {0} contacts.".format(contact_count))
    except Exception as e:
        log.info("Failed to query group.")
        log.error(e)
        sys.exit(-1)

    # needs a try-except here
    broadcast = broadcast_collection.insert_one({
        "groupId": group.id,
        "userIds": [user_id],
        "createdAt": datetime.datetime.now(),
        "recipientsCount": contact_count,
        "message": message,
        "media_url": media_url
    })

    broadcast_id = broadcast.inserted_id
    log.info("broadcast_id: {0}".format(broadcast_id))

    contacts = Contact.select().where(Contact.group_id == group.id)

    for contact in contacts:
        try:
            sid = copilot.send(contact.phone, message, locale, media_url)
            copilot.set_key(contact.phone, sid, broadcast_id, group.id, user_id, media_url)
            copilot.add_service(contact.phone, sid)
        except Exception as e:
            log.info("Failed to broadcast for contact: {0}".format(contact.phone))
            log.error(e)

    log.info("Successful broadcast to group_id = {0}, contacts = {1}".format(group.id, contact_count))
    sys.exit(1)


if __name__ == "__main__":
    action()
