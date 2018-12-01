"""
bulk_reply.py facilitates a bulk insert request of json into a group with contacts. Its slightly more performant then
other implementations, but is not as performant as it should be. Notably prepare_bulk_reply() contains multiple slow,
blocking actions, which is not something you want in a web application.
"""

import os
import sys
from datetime import datetime
from flask import request, jsonify

# Hack to allow imports of sibling directories given current project structure.
# A better layout could and should avoid this, I believe it just requires an __init__.py in the project root
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients.postgres import Postgres
from clients.mongo import Mongo


# Because of the hazardous potential of a rogue broadcast being created, we protect the endpoint with a static API key.
# This is a horrendous security implementation but it was quick to implement.
# LIKE REDIS_URL THE CODE WILL NOT WORK IF THE VARIABLE ISN'T IN THE CURRENT ENVIRONMENT.
SECRET_KEY = os.environ["PYTHON_APP_KEY"]
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)

# prepare_bulk_reply uses two clients to connect to external database services
# These services are connection from variables stored in the given environment will not work if
# the variables are not present, or if the databases are not configured accordingly (for example if the given database
# does not exist in Postgres)
POSTGRES_CLIENT = Postgres(os.environ["POSTGRES_DB"], os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"],
                               os.environ["POSTGRES_HOST"], os.environ["POSTGRES_PORT"])

Group = POSTGRES_CLIENT.get_model("Group")
Contact = POSTGRES_CLIENT.get_model("Contact")

MONGO_CLIENT = Mongo(os.environ["MONGO_URL"])
group_collection = MONGO_CLIENT.get_collection("groups")


def transform(contact, group_id):
    """Utility function returning a dictionary derived from the two parameters."""
    return {'phone': contact.get("phone"), 'group_id': group_id, 'sid': contact.get("sid")}


def prepare_bulk_reply():
    """
    prepare_bulk_reply() inserts into both a Postgres Database and Mongo Database models representing groups and
    contacts. This is not implemented performantly but has worked adequately for us. It suffers from the same shortfalls
    of many endpoints in this application, posing questions such as: what happens with large requests, memory bloat,
    what if the application is particularly busy, what happens if the operation fails half way through?

    It is not the least bit transactional or redundant.
    """
    data = request.get_json()

    # validate that this is an authenticated request
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    if {"groupName", "userId", "contacts"}.issubset(set(data.keys())):
        group_instance = Group.create(name=data["groupName"], reuse=True, broadcast_id=data["broadcastId"])
        modified_contacts = [transform(contact, group_instance.id) for contact in data["contacts"]]

        with POSTGRES_CLIENT.get_db().atomic():
            Contact.insert_many(modified_contacts).execute()

        group_collection.insert_one({
            "groupId": group_instance.id,
            "userIds": data["userId"],
            "createdAt": datetime.now(),
            "contactsCount": len(modified_contacts),
            "reuse": True,
            "broadcastId":data["broadcastId"],
            "name": data["groupName"]
        })

        return jsonify({"groupId": group_instance.id})

    return "not ok", 401
