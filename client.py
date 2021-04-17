import xmlrpc.client
import click
from concurrent.futures import ThreadPoolExecutor, as_completed
import random


@click.command()
@click.option('--url', default='null', help='Url to scan')
@click.option('--task', default='null', help='Task to do')
def start_connection(task, url):
    if task == 'null':
        task = input('Por favor le falta por introducir la tarea')
    if url == 'null':
        url = input('Por favor le falta darme una url con el formato http://localhost:10000/file.txt')
    server = xmlrpc.client.ServerProxy("http://localhost:9050/", allow_none=True)
    argument = task + ',' + url
    print(argument)


if __name__ == '__main__':
    #start_connection()
    server = xmlrpc.client.ServerProxy("http://localhost:10000/", allow_none=True)
    url = 'run-countwords,[http://localhost:8000/file1.txt], [http://localhost:8000/file1.txt], [http://localhost:8000/file1.txt], [http://localhost:8000/file2.txt], [http://localhost:8000/file2.txt]'
    print(server.addtask(url))
