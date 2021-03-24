import json
import multiprocessing
import redis
import requests
import random
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import logging
import time
from aux_functions import *

# variables globales que nos harán falta para la creación de los workers
JOB_ID = 1
worker_active = []
worker_active.append(True)
worker_active.append(True)
worker_active.append(True)
number_workers = 0

# definimos los datos para conectarnos a la base de datos
redis_host = 'localhost'
redis_port = 6379
redis_password = ""

# creamos la conexión y comprobamos que no haya error con un try catch
try:
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password)
except Exception as e:
    print(e)


def task_work(url, task, id):
    url = url.replace('[', '')
    url = url.replace(']', '')

    request = requests.get(url, allow_redirects=True)
    work_string = request.content.decode(request.encoding)
    words = 0
    if task == 'run-wordcount':
        words = count_words(work_string)
        r.rpush(id, words)
    elif task == 'run-countwords':
        words = count_rec(work_string)
        r.hmset(id, words)


def create_worker(num):
    while worker_active[num]:
        if r.llen('redisList') > 0:
            work = r.lpop('redisList')
            # decodificamos la tarea, ya que se guarda en binario en la lista de redis y la separamos por ','
            # el formato será ('Job_ID','Tarea_realizar','URl_1, URL_2, ...'
            if work:
                work = work.decode('ascii').split(',')
                # cogemos la longitud de la tarea spliteada, para saber si se trata de una tarea simple o múltiple
                length = len(work)
                print(length)
                if length > 3:
                    # cogemos los argumentos para tratar la multitarea
                    id = work.pop(0)
                    task = work.pop(0)
                    url = work.pop(0)
                    new_work = ''
                    for urls in work:
                        new_work = new_work + urls + ','
                    # eliminamos la última coma que se nos colocará en el array concatenado para volver a ponerlo en la lista
                    new_work = new_work[:-1]
                    new_work = new_work.replace('[', '')
                    new_work = new_work.replace(']', '')
                    # concatenamos el job_id, task y las url
                    task_new = id + ',' + task + ',' + new_work
                    # pusheamos en una lista del job_id la longitud que tiene la multitarea, 2 o más elementos
                    r.rpush(id, length - 2)
                    # sustituimos el valor que teníamos en posición 0 que era la tarea original con todas las url
                    # por una nueva con una url menos que será la que ha cogido este worker
                    r.lpush('redisList', task_new)
                    task_work(url, task, id)

                elif length == 3:
                    id = work.pop(0)
                    task = work.pop(0)
                    url = work.pop(0)
                    # si existe la lista con este job_id es porque es una multitarea, y tenemos que poner
                    # en la primera posición de la redisList una tarea que solo tenga 2 de longitud para que otro worker
                    # se prepare para devolver los resultados una vez acabados
                    task_work(url, task, id)
                    if r.exists(id):
                        r.lpush('redisList', id + ',' + task)
                        task_work(url, task, id)
                    # si no, simplemente quitamos la tarea de la lista
                    else:
                        id_job = id + '_ready'
                        task_work(url, task, id)
                else:
                    # quitamos ya la tarea de la lista definitivamente ya que este será el último paso
                    r.lpop('redisList')
                    id = work.pop(0)
                    task = work.pop(0)
                    # cogemos el valor de tareas a completar
                    task_pending = r.lpop(id).decode('ascii')
                    task_pending = int(task_pending)
                    task_completed = r.llen(id)
                    # comprobamos hasta que esten todas completadas, número tareas a completar = tareas completadas
                    while task_pending > task_completed:
                        task_completed = r.llen(id)
                    if task == 'run-wordcount':
                        # una vez todas completadas cogemos todos los valores de la lista y los sumamos
                        values = r.lrange(id, 0, task_pending - 1)
                        sum = 0
                        print(values)
                        for value in values:
                            sum = sum + int(value.decode('ascii'))
                        id_job = id + '_ready'
                        r.rpush(id_job, sum)
                    elif task == 'run-countwords':
                        values = r.hgetall(id)
                        print(values)


def create_workers(num_workers):
    processes = []
    for value in range(num_workers):
        process = multiprocessing.Process(target=create_worker, args=(value,))
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


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


def addtask(task):
    r.rpush('redisList', task)
    id = str(JOB_ID) + '_ready'
    while r.llen(id) == 0:
        time.sleep(0.1)
    return r.lpop(id)


# run server
def run_server(host="localhost", port=9000):
    r.flushall()
    create_workers(3)
    server_addr = (host, port)
    server = SimpleThreadedXMLRPCServer(server_addr)
    server.register_function(addtask, 'addtask')

    print("Server thread started. Testing server ...")
    print('listening on {} port {}'.format(host, port))

    server.serve_forever()


if __name__ == '__main__':
    run_server()
