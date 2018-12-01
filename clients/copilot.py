"""
copilot.py is an abstraction layer over the TwilioRestClient for use with Twilio's copilot service.

It contains a variety of helper methods breaking apart our process for sending SMS and MMS messages. Notably
it contains the logic for 'tracking' messages throughout our system using keys stored in Redis.

It also caches numbers such that they *should* not receive multiple messages from the same service twice. This feature
is FUNDAMENTALLY BROKEN and must be reimplemented some other way.
"""

import json
import time
import sys
import os
import socket

from twilio.rest import TwilioRestClient


# hack to allow us to import from sibling directory
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from meta.services import Services


class Copilot:
    def __init__(self, account_sid, account_token, redis_client):
        self.client = TwilioRestClient(account_sid, account_token)
        self.services = Services
        self.redis_client = redis_client

    # identify a service sid from the requested locale that has not yet been used with this number
    def get_service(self, number, locale):
        available = self.services[locale]
        taken = self.redis_client.lrange(number, 0, -1)

        for n in available:
            if n not in taken:
                return n

        return ""

    def add_service(self, number, sid):
        self.redis_client.rpush(number, sid)

    def set_key(self, to, sid, broadcast_id, group_id, user_id, media_url="", **kwargs):
        tracker_id = kwargs.get("tracker_id", "")

        key = '{{"to": "{0}", "sid": "{1}"}}'.format(to, sid)  # DOUBLE BRACKETS  FOR .FORMAT()
        value = json.dumps({"broadcastId": str(broadcast_id),"groupId":group_id,"userId":user_id,"mediaUrl": media_url,
                            "trackerId": tracker_id})

        self.redis_client.set(key, value)

    def send_sms(self, to, message, locale, messaging_service_sid):
        if messaging_service_sid == "":
            messaging_service_sid = self.get_service(to, locale)

        self.client.messages.create(to=to, body=message, messaging_service_sid=messaging_service_sid)
        return messaging_service_sid

    def send_mms(self, to, message, locale, media_url, messaging_service_sid):
        if messaging_service_sid == "":
            messaging_service_sid = self.get_service(to, locale)

        self.client.messages.create(to=to, body=message, media_url=media_url, messaging_service_sid=messaging_service_sid)
        return messaging_service_sid

    def send(self, to, message, locale, media_url, sid=""):
        if media_url != "":
            return self.send_mms(to, message, locale, media_url, sid)

        else:
            return self.send_sms(to, message, locale, sid)

    @staticmethod
    def test_send(to, message, locale):
        time.sleep(.2)  # simulate an api request for testing
        return "2394823094823"


if os.environ.get("DUMMY_TWILIO", 0) == "1":
    class Copilot:
        def __init__(self, account_sid, account_token, redis_client):
            self.client = TwilioRestClient(account_sid, account_token)
            self.services = Services
            self.redis_client = redis_client

        # identify a service sid from the requested locale that has not yet been used with this number
        def get_service(self, number, locale):
            available = self.services[locale]
            taken = self.redis_client.lrange(number, 0, -1)

            for n in available:
                if n not in taken:
                    return n

            return ""

        def add_service(self, number, sid):
            self.redis_client.rpush(number, sid)

        def set_key(self, to, sid, broadcast_id, group_id, user_id, media_url="", **kwargs):
            tracker_id = kwargs.get("tracker_id", "")

            key = '{{"to": "{0}", "sid": "{1}"}}'.format(to, sid)  # DOUBLE BRACKETS  FOR .FORMAT()
            value = json.dumps({"broadcastId": str(broadcast_id),"groupId":group_id,"userId":user_id,"mediaUrl": media_url,
                                "trackerId": tracker_id})

            self.redis_client.set(key, value)

        def __send(self, to, body, sid, meth="send_message"):
            values = {
                "method": meth,
                "args": {
                    "to": to,
                    "sid": sid,
                    "body": body
                }
            }
            jv = json.dumps(values)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", 8000))
            s.send(jv)
            s.close()

        def send_sms(self, to, message, locale, messaging_service_sid):
            if messaging_service_sid == "":
                messaging_service_sid = self.get_service(to, locale)
            self.__send(to, message, messaging_service_sid)
            
            return messaging_service_sid

        def send_mms(self, to, message, locale, media_url, messaging_service_sid):
            if messaging_service_sid == "":
                messaging_service_sid = self.get_service(to, locale)
            self.__send(to, message, messaging_service_sid)
            return messaging_service_sid

        def send(self, to, message, locale, media_url, sid=""):
            if media_url != "":
                return self.send_mms(to, message, locale, media_url, sid)
            else:
                return self.send_sms(to, message, locale, sid)

        @staticmethod
        def test_send(to, message, locale):
            time.sleep(.2)  # simulate an api request for testing
            return "2394823094823"

