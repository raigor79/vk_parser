import redis
import json

def conect():
    id_dev = redis.Connection()
    return id_dev



if __name__ == "__main__":
    id_dev = redis.Redis()
    di = {'1':'привет доброе утро','2':'проба'}
    id_dev.set('2', json.dumps(di))
    print(json.loads((id_dev.get('2'))))    