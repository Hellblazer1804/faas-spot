import os
import requests
from time import sleep

loop_count=0
while loop_count<3000:
    requests.get(f"{os.getenv('ROUTER_BASE_URL', 'http://localhost:31314')}/wf1")
    loop_count+=1
    sleep(1)