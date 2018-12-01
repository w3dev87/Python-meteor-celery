"""
Program entry-point.

main.py starts a minimally configured flask application, mounting the below imported API endpoints. Notably no higher
level configuration is done here as all client interfaces (such as databases) are instantiated as singletons upon their
initial imports within the endpoints that use them.

Note that the flask application is set to listen to an external host and port. This is because our infrastructure is
already behind a reverse-proxy (Nginx). This application should not be exposed without a proxy.

More custom configuration of Flask should be done. As well as the reverse-proxy standing in front of it.
"""


from flask import Flask
import os

from endpoints.broadcast import broadcast, broadcast_view, scatter, scatter_broadcast, celery_broadcast, celery_broadcast_state
from endpoints.image import image_upload, image_view
from endpoints.upload import upload_view, upload_file
from endpoints.bulk_reply import prepare_bulk_reply
from endpoints.replies import get_replies, new_reply
from endpoints.outbound import outbound_message
from endpoints.groups import create_group

app = Flask(__name__)
app.add_url_rule('/processor/broadcast_view', "broadcast_send", broadcast_view, methods=["GET"])
app.add_url_rule('/processor/broadcast_send', "broadcast", broadcast, methods=["POST"])
app.add_url_rule('/processor/scatter_broadcast', "scatter_broadcast", scatter_broadcast, methods=["POST"])
app.add_url_rule('/processor/scatter', 'scatter', scatter, methods=["POST"])
app.add_url_rule('/processor/celery_broadcast', "celery_broadcast", celery_broadcast, methods=["POST"])
app.add_url_rule('/processor/celery_broadcast_state/<task_id>', 'celery_broadcast_state', celery_broadcast_state, methods=["GET"])

app.add_url_rule('/processor/image_view', "image_upload", image_view, methods=["GET"])
app.add_url_rule('/processor/image_upload', "image", image_upload, methods=["POST"])
app.add_url_rule('/processor/upload_view', "upload_view", upload_view, methods=["GET"])
app.add_url_rule('/processor/upload_file', "upload_file", upload_file, methods=["POST"])

app.add_url_rule('/processor/prepare_bulk_reply', "prepare_bulk_reply", prepare_bulk_reply, methods=["POST"])
app.add_url_rule('/processor/get_replies', "get_replies", get_replies, methods=["POST"])

app.add_url_rule('/api/inbound', 'new_reply', new_reply, methods=["POST"])
app.add_url_rule('/api/outbound', 'outbound_message', outbound_message, methods=["POST"])

app.add_url_rule('/api/groups', "create_group", create_group, methods=["POST"])

if __name__ == '__main__':
    port = "PYTHON_APP_PORT" in os.environ and os.environ["PYTHON_APP_PORT"] or 80
    app.run(debug=False, host='0.0.0.0', port=port)
