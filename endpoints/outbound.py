"""
outbound.py is responsible for receiving and persisting requests from the Twilio API representing outgoing messages.
Messages are received generally in three states: queued, delivered, and undelivered.
"""

import os
import sys
import json
from flask import request
from bson.objectid import ObjectId
from twilio import twiml

pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients.mongo import Mongo
from clients._redis import Redis
from clients.logger import Logger

SECRET_KEY = os.environ["PYTHON_APP_KEY"]

# in order to log errors from the very beginning, we must have our logging set up first
LOGGER = Logger(os.environ["LOGGLY_TOKEN"]).get_logger()
MONGO_CLIENT = Mongo(os.environ["MONGO_URL"])
REPLY_COLLECTION = MONGO_CLIENT.get_collection("replies")
BROADCAST_COLLECTION = MONGO_CLIENT.get_collection("broadcasts")
REDIS_CLIENT = Redis(os.environ["REDIS_HOST"], os.environ["REDIS_PORT"], os.environ["REDIS_PASSWORD"]).get_client()


def outbound_message():
    """
    outbound_message() receives a multi-part-url-form-encoded http request and persists it to the relevent datastores
    """
    # convert combined multi-dict to dict, request body is URL ENCODED by Twilio
    data = {key: value for (key, value) in request.values.items()}
    LOGGER.info(json.dumps(data))

    # compose our redis key from values in request body
    key = '{{"to": "{0}", "sid": "{1}"}}'.format(data["To"], data["MessagingServiceSid"])  # notice re use data["To"]
    LOGGER.info(key)

    context = REDIS_CLIENT.get(key)
    if context is None:
        resp = twiml.Response()
        return str(resp)

    # merge our context data from redis with the request data
    data.update(json.loads(context.decode('utf-8')))
    LOGGER.info(data)

    # update the broadcast that this reply is for
    _id = ObjectId(data["broadcastId"])

    # compose mongodb filter and update params for the updateOne call
    # if trackerId value is present this is a reply to a nested broadcast and should update the nested broadcast
    tracker_id = data.get("trackerId", "")

    if tracker_id != "":
        field = "broadcasts.$.{0}".format(data["MessageStatus"])
        document_filter = {"_id": _id, "broadcasts.trackerId": tracker_id}
        update = {"$inc": {field: 1}}
    else:
        field = data["MessageStatus"]
        document_filter = {"_id": _id}
        update = {"$inc": {field: 1}}

    b = BROADCAST_COLLECTION.update_one(document_filter, update)
    LOGGER.info(b)

    resp = twiml.Response()
    return str(resp)
