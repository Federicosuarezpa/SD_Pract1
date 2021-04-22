import pickle
import multiprocessing
import redis
import requests
from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import time
from aux_functions import *

# variables globales que nos harán falta para la creación de los workers
worker_active = []
number_workers = 0
processes = []

# definimos los datos para conectarnos a la base de datos
redis_host = 'localhost'
redis_port = 6379
redis_password = ""

# creamos la conexión y comprobamos que no haya error con un try catch
try:
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password)
except Exception as e:
    print(e)


def task_work(url, task, id, op):
    url = url.replace('[', '')
    url = url.replace(']', '')

    request = requests.get(url, allow_redirects=True)
    work_string = request.content.decode(request.encoding)
    if task == 'run-wordcount':
        words = count_words(work_string)
        if op:
            id_job = id + '_ready'
            r.rpush(id_job, words)
        else:
            id_job = id + '_jobcompleted'
            r.rpush(id_job, words)
    elif task == 'run-countwords':
        words = count_rec(work_string)
        my_dict = pickle.dumps(words)
        if op:
            id_job = id + '_ready'
            r.set(id_job, my_dict)
        else:
            id_work = id + '_work'
            task_number = r.lpop(id_work)
            while not task_number:
                task_number = r.lpop(id_work)
            task_number = task_number.decode('ascii')
            task_number = int(task_number)
            task_name = str(task_number) + str(id)
            r.set(task_name, my_dict)
            pending = id + '_wait'
            r.rpush(pending, 'finished')
            task_number = task_number - 1
            r.rpush(id_work, task_number)


# mañana hacer pruebas para comprobar funcionamiento, arreglar errores y limpiar codigo, dejarlo mas legible
# poner workers a través de lista redis y ya estaría.
def create_worker(num):
    while r.lindex('redisListWorkers', num).decode('ascii') == 'True':
        time.sleep(0.5)
        if r.llen('redisList') > 0:
            work = r.lpop('redisList')
            # decodificamos la tarea, ya que se guarda en binario en la lista de redis y la separamos por ','
            # el formato será ('Job_ID','Tarea_realizar','URl_1, URL_2, ...'
            if work:
                work = work.decode('ascii').split(',')
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
                    new_work = new_work[:-1]
                    new_work = new_work.replace('[', '')
                    new_work = new_work.replace(']', '')
                    # concatenamos el job_id, task y las url
                    task_new = id + ',' + task + ',' + new_work
                    id_job = id + '_job'
                    # pusheamos en una lista del job_id la longitud que tiene la multitarea, 2 o más elementos
                    if not r.exists(id_job):
                        id_work = id + '_work'
                        r.rpush(id_job, length - 2)
                        r.rpush(id_work, length - 2)
                    # sustituimos el valor que teníamos en posición 0 que era la tarea original con todas las url
                    # por una nueva con una url menos que será la que ha cogido este worker
                    r.lpush('redisList', task_new)
                    task_work(url, task, id, 0)

                elif length == 3:
                    id = work.pop(0)
                    task = work.pop(0)
                    url = work.pop(0)
                    id_job = id + '_job'
                    # si existe la lista con este job_id es porque es una multitarea, y tenemos que poner
                    # en la primera posición de la redisList una tarea que solo tenga 2 de longitud para que otro worker
                    # se prepare para devolver los resultados una vez acabados
                    if r.exists(id_job):
                        r.lpush('redisList', id + ',' + task)
                        task_work(url, task, id, 0)
                    # si no, simplemente quitamos la tarea de la lista
                    else:
                        task_work(url, task, id, 1)
                elif length == 2:
                    # quitamos ya la tarea de la lista definitivamente ya que este será el último paso
                    r.lpop('redisList')
                    id = work.pop(0)
                    task = work.pop(0)
                    # cogemos el valor de tareas a completar
                    id_job = id + '_job'
                    task_pending = r.lpop(id_job)
                    while not task_pending:
                        task_pending = r.lpop(id_job)
                    if task_pending:
                        task_pending = task_pending.decode('ascii')
                        task_pending = int(task_pending)
                    if task == 'run-wordcount':
                        id_jobs = id + '_jobcompleted'
                        task_completed = 0
                        # comprobamos hasta que esten todas completadas, número tareas a completar = tareas completadas
                        while task_pending > task_completed:
                            task_completed = r.llen(id_jobs)
                            # una vez todas completadas cogemos todos los valores de la lista y los sumamos
                            values = r.lrange(id_jobs, 0, task_pending - 1)
                            sum = 0
                        for value in values:
                            sum = sum + int(value.decode('ascii'))
                        id_job = id + '_ready'
                        r.rpush(id_job, sum)
                    elif task == 'run-countwords':
                        count = dict()
                        array = []
                        pending = id + '_wait'
                        while r.llen(pending) < task_pending:
                            time.sleep(0.5)
                        for i in range(task_pending):
                            id_number = str(i + 1)
                            id_work = id_number + id
                            dicta = r.get(id_work)
                            dicta = pickle.loads(dicta)
                            array.append(dicta)
                        for i in range(task_pending):
                            for key in array[i]:
                                if key in count:
                                    count[key] += array[i].get(key)
                                else:
                                    count[key] = array[i].get(key)
                        id_job = id + '_ready'
                        count_dict = pickle.dumps(count)
                        r.set(id_job, count_dict)


def create_workers(n_workers):
    global processes
    for value in range(n_workers):
        process = multiprocessing.Process(target=create_worker, args=(r.llen('redisListWorkers'),))
        processes.append(process)
        r.rpush('redisListWorkers', 'True')
        process.start()


def delete_worker(index):
    if r.llen('redisListWorkers') != 0:
        r.lset('redisListWorkers', index, 'False')
    processes[index].join()

def list_workers():
    workers = []
    for worker in range(r.llen('redisListWorkers')):
        workers.append("WORKER_ID:" + str(worker) + " STATE: " + r.lindex('redisListWorkers', worker).decode('ascii'))

    return workers


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


def addtask(task):
    id_num = 0
    if r.exists('job_id'):
        id_num = str(r.lpop('job_id').decode('ascii'))
    else:
        id_num = 0
    task_do = task.split(',')
    task_do = task_do[0]
    task = str(id_num) + ',' + task
    r.rpush('redisList', task)
    id = str(id_num) + '_ready'
    id_num = int(id_num) + 1
    r.rpush('job_id', id_num)
    while not r.exists(id):
        time.sleep(0.1)
    if task_do == 'run-wordcount':
        value = r.lpop(id)
    else:
        dictio = r.get(id)
        value = pickle.loads(dictio)
    return value


# run server
def run_server(host="localhost", port=10000):
    r.flushall()
    create_workers(3)
    server_addr = (host, port)
    server = SimpleThreadedXMLRPCServer(server_addr, allow_none=True)
    server.register_function(addtask, 'addtask')
    server.register_function(create_workers, 'create_workers')
    server.register_function(delete_worker, 'delete_worker')
    server.register_function(list_workers, 'list_workers')

    print("Server thread started. Testing server ...")
    print('listening on {} port {}'.format(host, port))

    server.serve_forever()


if __name__ == '__main__':
    run_server()
