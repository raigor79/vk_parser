import redis
import json
import time


class RedisStore():
    def __init__(self, hostname='localhost', port=6379, socket_timeout=1, socket_connect_timeout=1, attempt=3):
        self.h_name = hostname
        self.port = port
        self.timeout = socket_timeout
        self.connect_timeout = socket_connect_timeout
        self.attempt=attempt
        self.delay = 0.1
        self.server = redis.Redis(
            host=self.h_name, 
            port=self.port, 
            socket_timeout=self.timeout, 
            socket_connect_timeout=self.connect_timeout
        )

    def get(self, key):
        for attm in range(self.attempt):
            try:
                return self.server.get(key) 
            except (TimeoutError, ConnectionError):
                time.sleep(self.delay*attm)   

    def set(self, key, value, expire=60):
        for attm in range(self.attempt):
            try:
                return self.server.set(key, value, expire)
            except (TimeoutError, ConnectionError):
                time.sleep(self.delay*attm)
    

if __name__ == "__main__":
    pass