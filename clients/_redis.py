import redis


class Redis:
    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.client = redis.StrictRedis(host=host, port=port, password=password)

    def get_client(self):
        return self.client
