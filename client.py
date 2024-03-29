import xmlrpc
import xmlrpc.client

import click

@click.command()
@click.argument('type_task', nargs=1, required=True)
@click.argument('task', nargs=1, required=True)
@click.argument('url', default='[http://localhost:8000/]')
@click.option('-n', default=1, type=int, nargs=1)
def start_connection(type_task, task, url, n):
    server = xmlrpc.client.ServerProxy("http://localhost:11000/", allow_none=True)

    if type_task == 'worker':
        if task == 'create':
            print('CREATING', n, 'WORKERS')
            server.create_workers(n)
        elif task == 'delete':
            print('DELETING WORKER WITH ID:', n)
            server.delete_worker(n)
        elif task == 'list':
            print('WORKERS LIST')
            print(server.list_workers())
    elif type_task == 'job':
        argument = task + ',' + url
        print('ADDING TASK:', argument)
        print(server.addtask(argument))


if __name__ == '__main__':
    start_connection()
