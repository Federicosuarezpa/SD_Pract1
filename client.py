import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
server = xmlrpc.client.ServerProxy("http://localhost:9050/", allow_none=True)
url = 'run-countwords,[http://localhost:8000/file1.txt'
print(server.addtask(url))