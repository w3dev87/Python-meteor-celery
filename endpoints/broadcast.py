"""
broadcast.py contains API endpoints regarding the manipulation of broadcasts.

It contains multiple endpoints for creating broadcasts, representing multiple approaches I took to doing so.

Notably the celery_broadcast() endpoint is the one currently in use by our application and is really the only one
that matters.

All other functions are mostly dead code.
"""

import os
import subprocess
from flask import render_template, request, jsonify
from celery import Celery

# In use by this module is a Celery client which utilizes a redis broker that is hosted externally via compose. The
# connection string is obtained from the OS environment. THIS CODE WILL NOT WORK IF 'REDIS_URL' IS NOT SET VALIDLY.
redis_url = os.environ["REDIS_URL"] + "/0"
app = Celery('tasks', backend=redis_url, broker=redis_url)
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_RESULT_SERIALIZER='json',
)

# Because of the hazardous potential of a rogue broadcast being created, we protect the endpoint with a static API key.
# This is a horrendous security implementation but it was quick to implement.
# LIKE REDIS_URL THE CODE WILL NOT WORK IF THE VARIABLE ISN'T IN THE CURRENT ENVIRONMENT.
SECRET_KEY = os.environ["PYTHON_APP_KEY"]
BASE_DIR = os.path.normpath(__file__ + os.sep + os.pardir + os.sep + os.pardir)


def broadcast_view():
    """Render a static template to test functionality during development"""
    return render_template("broadcast.html")


def broadcast():
    """Launch a broadcast as a subprocess of the current process. This broadcast does not scale, at all."""
    data = request.get_json()

    # validate that this is an authenticated request
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    # validate that all required data fields are present
    if {"groupId", "message", "locale", "userId", "token"}.issubset(set(data.keys())):
        subprocess.Popen(["python3", os.path.join(BASE_DIR, "tasks", "broadcast.py"), data["groupId"], data["message"],
                          data["locale"], data["userId"], data["mediaUrl"]])

        return "ok", 200

    return "not ok", 401


def celery_broadcast():
    """
    Make a celery RPC call to a listening worker to execute the given broadcast. These workers are represented by the
    celery-app repository. Due to our broker (Redis) and my limited knowledge of Celery's internals, there is a certain
    propensity for broadcasts to be duplicated. If this implementation is to remain in operation, it needs to be further
    understood.

    Questions that should be answered:
    * what happens if there is no listening worker?
    * what happens if the broker is in a weird state?
    * what happens if the worker ACKs the task but does not finish it?
    * what happens if the Celery client times out?
    * what happens if this process or the worker process hangs or runs out of memory?
    """
    data = request.get_json()

    # validate that this is an authenticated request
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    # validate that all required data fields are present
    if {"groupId", "message", "locale", "userId", "mediaUrl", "trackerId"}.issubset(set(data.keys())):
        task = app.send_task('tasks.broadcast.broadcast_task', args=[data["groupId"], data["message"], data["locale"],
                          data["userId"], data["mediaUrl"]])
        return jsonify({"task_id": task.id}), 200

    return "not ok", 401


def celery_broadcast_state(task_id):
    """Retrieve the state and some info for a given celery Task"""
    task = app.AsyncResult(task_id)
    return jsonify({"state": task.state, "info": task.info}), 200


def scatter_broadcast():
    """
    Scatter broadcast was a test implementation that launches a subprocess that launches subprocesses in order to
    accommodate larger broadcasts. It does not scale meaningfully, as such work should be distributed. Futhermore
    I am not entirely clear on the full behavior of subprocesses, what if the master process dies? Memory issues? This
    implementation should not be used in production.
    """
    data = request.get_json()

    # validate that this is an authenticated request
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    # validate that all required data fields are present
    if {"groupId", "message", "locale", "userId", "mediaUrl", "trackerId"}.issubset(set(data.keys())):
        subprocess.Popen(["python3", os.path.join(BASE_DIR, "tasks", "scatter_broadcast.py"),
                          data["groupId"], data["message"], data["locale"],
                          data["userId"], data["mediaUrl"], data["trackerId"]])

        return "ok", 200

    return "not ok", 401


def scatter():
    """
    Scatter represents the sub-sub process action initiated by the scatter_broadcast call(). scatter() does
    the actual work in that implementation.
    """
    data = request.get_json()
    if "token" not in data or data["token"] != SECRET_KEY:
        return "not ok", 401

    if {"groupId", "message", "locale", "mediaUrl", "trackerId", "broadcastId", "userId", "chunk", "chunkSize"}.issubset(set(data.keys())):
        subprocess.Popen(["python3", os.path.join(BASE_DIR, "tasks", "scatter.py"),
                          data["groupId"], data["message"], data["locale"],
                          data["mediaUrl"], data["trackerId"], data["broadcastId"],
                          data["userId"], data["chunk"], data["chunkSize"]])

        return "ok", 200

    return "not ok", 401
