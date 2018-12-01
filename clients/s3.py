import tinys3


class S3:
    def __init__(self, access_key, secret_key):
        self.client = tinys3.Connection(access_key, secret_key, tls=True)

    def upload(self, file_name, file, bucket_name):
        self.client.upload(file_name, file, bucket_name)
