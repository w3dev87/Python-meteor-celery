"""
replies.py is responsible for handling requestions from Twilio's API representing end user SMS replies. These replies
are persisted piece-wise in three different databases: postgres, mongoDB, redis. Obvious having a segmented data model
is not ideal, but is necessary both for funcationality and performance reasons. Generally speaking Mongodb is used in
the view layer, Postgres for general persistance, and Redis for performance constrained operations.
"""

import os
import sys
import json
from datetime import datetime
from flask import request
from bson.json_util import dumps
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


def get_replies():
    """get_replies queries reply documents from MongoDB related to a corresponding BroadcastId"""
    data = request.get_json()

    # validate that this is an authenticated request
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    # query all documents from our mongo db with given broadcastId
    if {"broadcastId"}.issubset(set(data.keys())):
        replies = REPLY_COLLECTION.find({"broadcastId": data["broadcastId"]})
        return dumps(replies)

    return "not ok", 401


def new_reply():
    """
    new_reply takes a multi-part-url-form-encoded http request and persists key attributes into the relevant datastores
    """
    # convert immutable multi-dict to dict, data is URL ENCODED by Twilio
    data = {key: value for (key, value) in request.values.items()}
    LOGGER.info(json.dumps(data))

    # compose our redis key from values in request body
    key = '{{"to": "{0}", "sid": "{1}"}}'.format(data["From"], data["MessagingServiceSid"])
    LOGGER.info(key)

    # merge our data from redis with the request data
    context = REDIS_CLIENT.get(key).decode("utf-8")  # by default returns type: Bytes
    LOGGER.info(context)

    if context is None:
        resp = twiml.Response()
        return str(resp)

    data.update(json.loads(context))
    LOGGER.info(data)

    # update the broadcast that this reply is for
    _id = ObjectId(data["broadcastId"])

    # compose mongodb filter and update params for the updateOne call
    # if trackerId value is present this is a reply to a nested broadcast and should update the nested broadcast
    tracker_id = data.get("trackerId", "")
    if tracker_id != "":
        document_filter = {"_id": _id, "broadcasts.trackerId": tracker_id}
        update = {"$inc": {"broadcasts.$.repliesCount": 1}}
    else:
        document_filter = {"_id": _id}
        update = {"$inc": {"repliesCount": 1}}

    b = BROADCAST_COLLECTION.update_one(document_filter, update)
    LOGGER.info(b)

    data["createdAt"] = datetime.now()
    data["needsReply"] = True
    r = REPLY_COLLECTION.insert_one(data)
    LOGGER.info(r)

    resp = twiml.Response()
    return str(resp)
