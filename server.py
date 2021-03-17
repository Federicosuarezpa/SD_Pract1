import multiprocessing
import redis

import time



redis_host = 'localhost'
redis_port = 6379
redis_pasword = ""


def worker(num):
    while (True):
        print(num)
        time.sleep(1)


if __name__ == '__main__':
    try:
        r = redis.StrictRedis(host=redis_host,port=redis_port,password=redis_pasword)
        r.rpush('NOSQLLIST', "hello")

        r.rpush('NOSQLLIST', 'asdads')

        msg = r.lpop('NOSQLLIST')
        print(msg)
        msg = r.lpop('NOSQLLIST')
        print(msg)
    except Exception as e:
        print(e)

