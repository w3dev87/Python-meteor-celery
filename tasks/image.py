"""
image.py is a one-off script intended to be run as a sub process. It reads file from a sub-directory, uploads the file
to AWS and deletes the file when complete.

If the process errors out mid-execution, or some other problem occurs, the file may be orphaned and the task may never
be completed at all.

Ideally such an operation would be implemented transactionally and atomically. Unfortunately this was a 'good-enoug'
approach to the features needed.

This is used in production.
"""

import os
import sys
from datetime import datetime

# hack to allow us to import from sibling directories
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from core.BaseTask import BaseTask
from clients.logger import Logger

# in order to log errors from the very beginning, we must have our logging set up first
LOGGER = Logger(os.environ["LOGGLY_TOKEN"]).get_logger()

# project structure is messed up, keep a reference to the ROOT_DIR in /handstack-python
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)

# s3 bucket name
BUCKET_NAME = os.environ["AWS_BUCKET_NAME"]


class Image(BaseTask):
    map = ["file_name", "image_name", "user_id"]

    def __init__(self, *args):
        super(Image, self).__init__(*args)

    def run(self):
        file_path = os.path.join(BASE_DIR, "images", self.params.get("file_name"))
        thumb_file_path = os.path.join(BASE_DIR, "images", "thumb_" + self.params.get("file_name"))

        try:
            with open(file_path, "rb") as f_one:
                self.s3_client.upload(self.params.get("file_name"), f_one, BUCKET_NAME)
                f_one.close()

            with open(thumb_file_path, "rb") as f_two:
                self.s3_client.upload("thumb_" + self.params.get("file_name"), f_two, BUCKET_NAME)
                f_two.close()

        except Exception as e:
            LOGGER.error("Failed to upload image {0} for user {1}".format(self.params.get("file_name"),
                                                                          self.params.get("user_id")))
            LOGGER.info(e)
            sys.exit(-2)

        # save to mongodb
        image_collection = self.mongo_client.get_collection("images")

        try:
            image_collection.insert_one({
                "userIds": [self.params.get("user_id")],
                "createdAt": datetime.now(),
                "url": "https://s3.amazonaws.com/handstack-test2/{0}".format(self.params.get("file_name")),
                "thumb_url": "https://s3.amazonaws.com/handstack-test2/thumb_{0}".format(self.params.get("file_name")),
                "name": self.params.get("image_name")
            })

            os.remove(file_path)
            os.remove(thumb_file_path)

        except Exception as e:
            LOGGER.error("Failed to persist image record {0} for user {1}".format(self.params.get("file_name"),
                                                                                  self.params.get("user_id")))
            LOGGER.error(e)

        LOGGER.info("Image uploaded successfully: {0}, user_id: {1}".format(self.params.get("file_name"),
                                                                            self.params.get("user_id")))
        sys.exit(1)


if __name__ == "__main__":
    try:
        Image(sys.argv[1:]).run()

    except Exception as outer_e:
        LOGGER.error("Failed to complete process Image, args: {0}".format(sys.argv))
        LOGGER.info(outer_e)
