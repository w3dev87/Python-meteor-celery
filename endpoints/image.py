"""
image.py launches tasks as a subprocess intended to store images to AWS. This is very much a "good enough"
implementation as we store new images extremely rarely.
"""

import os
import subprocess
from flask import render_template, request
from werkzeug.utils import secure_filename
from random import randrange
from PIL import Image
from resizeimage import resizeimage

# 'PYTHON_APP_KEY' is an environmental variable used to keep out unauthorized access from the API
# THIS CODE WILL NOT WORK IF THE VARIABLE IS NOT PRESENT IN THE CURRENT ENVIRONMENT.
SECRET_KEY = os.environ["PYTHON_APP_KEY"]
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)


# Render static template for testing during development.
def image_view():
    return render_template("image.html")


def image_upload():
    """
    image_upload() reads a file from the http request and saves it to the local filesystem. It then launches a
    subprocess to handle the file, this action includes deleting the file from the file system and uploading it to AWS.
    Notable if something fails half way through the file may be orphaned in the file system, and the task has a
    propensity to not return an error if something goes wrong during the subprocess, which is essentially just an
    asynchronous action.
    """
    file = request.files['file']

    # validate that this is an authenticated request
    if "token" not in request.form or request.form["token"] != SECRET_KEY:
        return "not ok", 401

    # validate that all required data fields are present
    if file and request.form["imageName"] and request.form["userId"]:
        filename = str(randrange(0, 10000)) + file.filename
        s_filename = secure_filename(filename)
        s_filename_thumb = secure_filename("thumb_" + filename)

        try:
            # save the file in a raw state
            file_path = os.path.join(BASE_DIR, "images", s_filename)
            file.save(file_path)

            # save the file in a cropped state
            with Image.open(file_path) as image:
                cropped_image_thumb = resizeimage.resize_cover(image, [120, 80])
                cropped_image_thumb.save(os.path.join(BASE_DIR, "images", s_filename_thumb))

        except Exception as e:
            print(e)
            return "not ok", 401

        subprocess.Popen(["python3", os.path.join(BASE_DIR, "tasks", "image.py"), s_filename, request.form["imageName"], request.form["userId"]])
        return "ok", 200

    return "Missing file or imageName", 401