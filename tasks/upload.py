"""
upload.py is a one-off script intended to be run in a subprocess. It is responsible for reading a file from the local
filesystem and saving it in multiple databases in multiple formats. Obviously such a skewed data model is not ideal and
should be much simpler and more uniform.

Like many other parts of this codebase it is not atomic, transactional, or redundant, when it should be. This is used
in production.

However I am somewhat proud of how it translates a .csv file to our schema some-what dynamically. The CSV can hold
a variety of headers in any order and have it translated fairly well.
"""

import csv
import datetime
import json
import phonenumbers
import os
import sys

# hack to allow us to import from sibling directories
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from core.BaseTask import BaseTask
from clients.logger import Logger

# in order to log errors from the very beginning, we must have our logging set up first
LOGGER = Logger(os.environ["LOGGLY_TOKEN"]).get_logger()

# project structure is messed up, keep a reference to the ROOT_DIR in /handstack-python
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)

# bulk insert rows from our csv files into postgres in chunks of size BULK_INSERT_CHUNK_SIZE
BULK_INSERT_CHUNK_SIZE = 500


class Upload(BaseTask):
    map = ["file_name", "group_name", "user_id", "tracker_id"]

    def __init__(self, *args):
        super(Upload, self).__init__(*args)

    def run(self):
        LOGGER.info("Upload of {0} for user {2} name {1}".format(self.params.get("file_name"),
                                                                 self.params.get("group_name"),
                                                                 self.params.get("user_id")))

        file_path = os.path.join(BASE_DIR, "csvs", self.params.get("file_name"))
        with open(file_path) as csvfile:
            reader = csv.reader(csvfile)

            csv_legend = {
                "name": ["name", "Name", "Full Name", "Full_Name", "full_name", "full name"],
                "firstName": ["first name", "first_name", "firstName", "First Name", "first", "FirstName"],
                "lastName": ["last name", "last_name", "lastName", "Last Name", "last", "LastName"],
                "phone": ["cellphone", "Cell Phone", "Cell", "Cell_Phone", "phone number", "phone_number",
                          "phoneNumber", "Phone Number",
                          "Number", "number", "phone", "Phone"],
                "email": ["email", "email_address", "emailAddress", "Email Address", "Email"],
                "vanid": ["VANID", "vanid", "vanId", "VanId"]
            }

            csv_headers = {}

            # retrieve our PeeWee Models from our postgres_client
            Group = self.postgres_client.get_model("Group")
            Contact = self.postgres_client.get_model("Contact")

            # instantiate and persist row
            group_instance = Group.create(name=self.params.get("group_name"))

            # parse all the contacts from the csvfile using our csv_legend and the csv_headers found in the first LOF
            contacts = []
            for index, row in enumerate(reader):
                # first row of csv contains headers we can map to our legend dict in order to identify header indices
                if index == 0:
                    for key, values in csv_legend.items():
                        for i, value in enumerate(row):
                            if value in values:
                                csv_headers[i] = key

                    LOGGER.info("headers " + json.dumps(csv_headers, ensure_ascii=False))

                    # if required header is not in csv, exit
                    if "phone" not in csv_headers.values():
                        sys.exit(-3)

                    continue

                # dict mapping values found for a single contact to a key in csv_legend
                c_dict = {}
                for i, value in enumerate(row):
                    if i in csv_headers:
                        c_dict[csv_headers[i]] = value

                # check if c_dict contains a first name and last name, if so we have a complete name for the contact
                name = c_dict["firstName"] + " " + c_dict["lastName"] if {"firstName", "lastName"}.issubset(set(c_dict.keys())) else "anonymous"

                if "name" in c_dict:
                    name = c_dict["name"]  # override name if we have an explicit "name" attribute

                email = c_dict["email"] if "email" in c_dict else None

                vanid = c_dict["vanid"] if "vanid" in c_dict else None

                # normalize phone number to E.164, if not possible this is an invalid contact and we should move on
                try:
                    number = phonenumbers.format_number(phonenumbers.parse(c_dict["phone"], "US"),
                                                        phonenumbers.PhoneNumberFormat.E164)

                except Exception as inner_e:
                    LOGGER.info(inner_e)
                    continue

                contacts.append({'name': name, 'phone': number, 'group_id': group_instance.id, 'email': email, 'vanid': vanid})

            # friends don't let friends leave open file handles
            csvfile.close()

            try:
                LOGGER.info("Saving of {0} for user {2} name {1} g.id {3}".format(self.params.get("file_name"),
                                                                                  self.params.get("group_name"),
                                                                                  self.params.get("user_id"),
                                                                                  group_instance.id))
                with self.postgres_client.get_db().atomic():
                    for idx in range(0, len(contacts), BULK_INSERT_CHUNK_SIZE):
                        Contact.insert_many(contacts[idx:idx + BULK_INSERT_CHUNK_SIZE]).execute()

                group_collection = self.mongo_client.get_collection("groups")
                group_collection.insert_one({
                    "groupId": group_instance.id,
                    "userIds": [self.params.get("user_id")],
                    "createdAt": datetime.datetime.now(),
                    "contactsCount": len(contacts),
                    "name": self.params.get("group_name"),
                    "reuse": False
                })

                tracker_collection = self.mongo_client.get_collection("trackers")
                tracker_collection.update_one({"_id": self.params.get("tracker_id")}, {"$set": {"progress": 100}})

                os.remove(file_path)
                LOGGER.info("V2 Completion of {0} for user {2} name {1} g.id {3}".format(self.params.get("file_name"),
                                                                                      self.params.get("group_name"),
                                                                                      self.params.get("user_id"),
                                                                                      group_instance.id))

                sys.exit(1)

            except Exception as inner_e:
                LOGGER.info("Failure of {0} for user {2} name {1} g.id {3}".format(self.params.get("file_name"),
                                                                                   self.params.get("group_name"),
                                                                                   self.params.get("user_id"),
                                                                                   group_instance.id))
                LOGGER.error(inner_e)
                sys.exit(-4)


if __name__ == "__main__":
    try:
        Upload(sys.argv[1:]).run()

    except Exception as outer_e:
        LOGGER.error("Unable to launch task: {0}. Arguments provided: {1}, error: {2}".format(__file__, sys.argv, outer_e))

