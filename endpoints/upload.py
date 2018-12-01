"""
upload.py is responsible for handling the upload of files. Similar to image.py, files are stored temporarily in the
local file system and then processed in a subprocess where they are then deleted and stored externally in AWS.

Like images, this implementation suffers from a lack of transactional or redundant behavior, operations may fail half
way through for a variety of reasons and files may be orphaned in the local file system.

A different implementation should ultimately be used...
"""

import os
import subprocess

from flask import render_template, request
from werkzeug.utils import secure_filename
from random import randrange

SECRET_KEY = os.environ["PYTHON_APP_KEY"]
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)


def upload_view():
    """Render view for testing of functionality during development"""
    return render_template("upload.html")


def upload_file():
    """
    upload_file() reads a file from an http request and saves it to the local filesystem. It then launches a sub-process
    to process the file.
    """
    file = request.files['file']

    # validate that this is an authenticated request
    if "token" not in request.form or request.form["token"] != SECRET_KEY:
        return "not ok", 401

    # validate that all required data fields are present
    if file and request.form["groupName"] and request.form["userId"]:
        tracker_id = request.form["trackerId"] if "trackerId" in request.form else ""  # tracker id may not be present
        filename = request.form["groupName"] + str(randrange(0, 10000))
        s_filename = secure_filename(filename)

        try:
            file.save(os.path.join(BASE_DIR, "csvs", s_filename))
        except Exception as e:
            print(e)

        subprocess.Popen(["python3", os.path.join(BASE_DIR, "tasks", "upload.py"), s_filename, request.form["groupName"],
                          request.form["userId"], tracker_id])

        return "ok", 200

    return "Missing file or groupName", 401
