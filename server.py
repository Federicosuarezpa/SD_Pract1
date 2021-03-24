import json
import multiprocessing
import redis
import requests
from xmlrpc.server import SimpleXMLRPCServer
import logging
from aux_functions import *

# variables globales que nos harán falta para la creación de los workers
JOB_ID = 0
workers_active = []
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


def create_worker(num):
    while workers_active[num]:
        if r.llen('redisList') > 0:
            work = r.lrange('redisList', 0, 0)
            # decodificamos la tarea, ya que se guarda en binario en la lista de redis y la separamos por ','
            # el formato será ('Job_ID','Tarea_realizar','URl_1, URL_2, ...'
            work = work[0].decode('ascii').split(',')
            print(work)
            # cogemos la longitud de la tarea spliteada, para saber si se trata de una tarea simple o múltiple
            length = len(work)
            if length > 3:
                # cogemos los argumentos para tratar la multitarea
                id = work.pop(0)
                task = work.pop(0)
                url = work.pop(0)
                new_work = ''
                for urls in work:
                    new_work = new_work + urls + ','
                # eliminamos la última coma que se nos colocará en el array concatenado para volver a ponerlo en la lista
                new_work = new_work[:-2]
                # concatenamos el job_id, task y las url
                task_new = id + ',' + task + ',' + new_work
                # pusheamos en una lista del job_id la longitud que tiene la multitarea, 2 o más elementos
                r.rpush(id, length)
                # sustituimos el valor que teníamos en posición 0 que era la tarea original con todas las url
                # por una nueva con una url menos que será la que ha cogido este worker
                r.lset('redisList', 0, task_new)
            elif length == 3:
                id = work.pop(0)
                task = work.pop(0)
                url = work.pop
                # si existe la lista con este job_id es porque es una multitarea, y tenemos que poner
                # en la primera posición de la redisList una tarea que solo tenga 2 de longitud para que otro worker
                # se prepare para devolver los resultados una vez acabados
                if r.exists(id):
                    r.lset('redisList', 0, id + ',' + task)
                # si no, simplemente quitamos la tarea de la lista
                else:
                    r.lpop('redisList')
            else:
                # quitamos ya la tarea de la lista definitivamente ya que este será el último paso
                r.lpop('redisList')
                id = work.pop(0)
                print(work)
                # cogemos el valor de tareas a completar
                task_pending = r.lpop(id).decode('ascii')
                task_pending = int(task_pending)
                task_completed = r.llen(id)
                # comprobamos hasta que esten todas completadas, número tareas a completar = tareas completadas
                while task_pending > task_completed:
                    task_completed = r.llen(id)
                # una vez todas completadas cogemos todos los valores de la lista y los sumamos
                values = r.lrange(id, 0, task_pending).decode('ascii')
                sum = 0
                for value in values:
                    sum = sum + int(value)
                print(sum)


def add_task(task):
    r.rpush('redisList', task)
    #msg = r.lpop('redisList')


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
    create_workers(3)
    server = SimpleXMLRPCServer(('localhost', 9000),
                                logRequests=True,
                                allow_none=True)
    server.register_introspection_functions()
    server.register_multicall_functions()

    server.register_function(add_task)

    try:
        print('Use Control-C to exit')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Exiting')
    r.ltrim('redisList', -1, 0)
