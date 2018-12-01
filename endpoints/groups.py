"""
groups.py is responsible for creating new group objects, which are represented in both a Postgres and MongoDB database.
"""

import os
import sys
import datetime
import phonenumbers

from flask import request, jsonify

# hack to allow us to import from sibling directories
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients.postgres2 import postgres_db, Group, Contact
from clients.mongo2 import get_collection


def create_group():
    data = request.get_json()

    # guard statement which errors out if required data fields are not in the request
    if not {"name", "contacts", "userId"}.issubset(set(data.keys())):
        return "not ok", 401

    # get a db connection from our pool
    postgres_db.connect()

    # create and save a group model
    group_instance = Group.create(name=data["name"])

    # instantiate and compose an array of objects corresponding to Contact objects derived from data["contacts"]
    contacts = []
    for contact in data["contacts"]:
        # normalize number eo E.164
        number = phonenumbers.format_number(phonenumbers.parse(contact.get("number", ""), "US"),
                                            phonenumbers.PhoneNumberFormat.E164)

        contacts.append({'name': contact.get("name", ""), 'phone': number,
                         'group_id': group_instance.id, 'email': contact.get("email", "")})

    Contact.insert_many(contacts).execute()

    # create and save a record to our mongo database now that it has been persisted successfully to postgres
    group_collection = get_collection("groups")
    group_collection.insert_one({
        "groupId": group_instance.id,
        "userIds": [data["userId"]],
        "createdAt": datetime.datetime.now(),
        "contactsCount": len(contacts),
        "name": data["name"],
        "reuse": False
    })

    # return db connection to our pool
    postgres_db.close()

    return jsonify({"data": data, "contacts": contacts})
