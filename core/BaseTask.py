"""
BaseTask.py is an abstract base class for a piece of this application I call a 'task'. A task is intended to be run
in a sub process asynchronously to an http request. It is not a very good implementation because it lacks error handling
and is not the least bit transactional or redundant.

Furthermore this is somewhat of a forced usage of OOP, and was really an experiment in python features. There is no
reason 'tasks' couldn't have been implemented without OOP is a more functional manner.

It is kind of nice not to have to redeclare client database functionality when defining classes though...
"""

import os
import sys
from abc import ABC, abstractclassmethod, abstractproperty

# hack to allow us to import from sibling directory
pathup = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0, pathup)

from clients._redis import Redis
from clients.copilot import Copilot
from clients.mongo import Mongo
from clients.postgres import Postgres
from clients.s3 import S3


class BaseTask(ABC):
    """
    A simple base class for describing tasks and eliminating some boiler plate code.
    """
    params = {}

    redis_client = Redis(os.environ["REDIS_HOST"], os.environ["REDIS_PORT"], os.environ["REDIS_PASSWORD"])
    twilio_client = Copilot(os.environ["TWILIO_ACCOUNT_ID"], os.environ["TWILIO_AUTH_TOKEN"], redis_client.get_client())
    mongo_client = Mongo(os.environ["MONGO_URL"])

    postgres_client = Postgres(os.environ["POSTGRES_DB"], os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"],
                               os.environ["POSTGRES_HOST"], os.environ["POSTGRES_PORT"])

    s3_client = S3(os.environ["AWS_KEY"], os.environ["AWS_TOKEN"])

    def __init__(self, *args):
        #  ensure this task received the desired number parameters
        if len(*args) != len(self.map):
            raise ValueError("Too many or too few arguments provided. {0} != {1}".format(len(*args), len(self.map)))

        #  build a dict for parameter accessed based on the map attribute defined in child classes
        for index, arg in enumerate(*args):
            self.params[self.map[index]] = arg

    @abstractproperty
    def map(self):
        pass

    @abstractclassmethod
    def run(self): pass
