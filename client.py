import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
server = xmlrpc.client.ServerProxy("http://localhost:9000/", allow_none=True)
url = '1,run-countwords,[http://localhost:8000/file1.txt, http://localhost:8000/file2.txt'
print(server.addtask(url))