import json
import multiprocessing
import redis
import requests
from aux_functions import *

# variables globales que nos harán falta para la sincronización de procesos y tareas
job_active = ''
array = []
workers_active = []
processes = []
number_workers_needed = 0
number_workers_finished = 0
work_active = []
number_workers = 5
result = 0

# definimos los datos para conectarnos a la base de datos
redis_host = 'localhost'
redis_port = 6379
redis_password = ""

# creamos la conexión y comprobamos que no haya error con un try catch
try:
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password)
except Exception as e:
    print(e)


def create_worker(num):
    while workers_active[num]:
        global number_workers_needed
        global number_workers_finished
        global job_active
        if number_workers_needed > 1:
            global result
            work_job = array.pop()
            function = job_active
            request = requests.get(work_job, allow_redirects=True)
            work_string = request.content.decode(request.encoding)
            if function == 'run-wordcount':
                words = count_words(work_string)
                result = result + words
            else:
                words = count_rec(work_string)
                for value in words:
                    if value in result:
                        result[value] += 1
                    else:
                        result[value] = 1
        elif number_workers_needed == 1:
            loop = True
            while loop:
                if number_workers_finished == 0:
                    r.rpush('finishedJob', result)
        else:
            work_job = r.lpop('redisList')
            work_splited = work_job.split(',')
            if len(work_job) > 3 and len(array) == 0:
                number_workers_needed = len(work_job) - 1
                number_workers_finished = len(work_job) - 1
                job_active = work_splited.pop(1)
            if len(work_job) > 3 and len(array) > 0:
                r.rpush('redisList', work_job)
            else:
                function = work_splited[1]
                url = work_splited[2]
                request = requests.get(url, allow_redirects=True)
                work_string = request.content.decode(request.encoding)
                if function == 'run-wordcount':
                    words = count_words(work_string)
                    r.rpush('finishedJob', words)
                else:
                    words = count_rec(work_string)
                    r.rpush('finishedJob', words)



def add_task(task):
    r.rpush('redisList', task)
    msg = r.lpop('redisList')


def create_workers(num_workers):
    for value in range(num_workers):
        process = multiprocessing.Process(target=create_worker, args=(value,))
        work_active[value] = True
        processes.append(process)
        process.start()


def delete_worker():
    global number_workers
    if r.llen('redisList') == 0:
        work_active[number_workers - 1] = False
        number_workers = number_workers - 1


def add_worker():
    global number_workers
    number_workers = number_workers + 1
    work_active[number_workers - 1] = True


if __name__ == '__main__':
    r.rpush('redisList', '1,run-asdasd,http://localhost:8000/file1.txt')
    work_job = r.lpop('redisList')
    work_job = work_job.decode('ascii')
    work_splited = work_job.split(',')
    print(len(work_splited))
    if len(work_splited) > 3 and len(array) == 0:
        number_workers_needed = len(work_job) - 1
        number_workers_finished = len(work_job) - 1
        job_active = work_splited.pop(1)
    if len(work_job) > 3 and len(array) > 0:
        r.rpush('redisList', work_job)
    else:
        function = work_splited[1]
        url = work_splited[2]
        request = requests.get(url, allow_redirects=True)
        work_string = request.content.decode(request.encoding)
        if function == 'run-wordcount':
            words = count_words(work_string)
            r.rpush('finishedJob', words)
        else:
            print('hola')
            words = count_rec(work_string)
            print(words)
            r.hset('finishedJob', 1, words)
    msg = json.loads(r.hget('finishedJob', 1))
    print(msg)