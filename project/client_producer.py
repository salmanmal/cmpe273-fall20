import zmq
import time
import sys
from itertools import cycle
from hrw import select_node as hrw_select_node
from consistent_hashing import ConsistentHashing

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10000):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        next(pool).send_json(data)
        # time.sleep(1)
    print("Done")


def generate_data_consistent_hashing(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)
    ch=ConsistentHashing(servers)
    count=[0,0,0,0]
    for num in range(0,10000):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        node = ch.select_node(data["key"])
        count[node]+=1
        print(f"Consistent hashing :: Sending data:{data} to server {servers[node]}")
        producers[servers[node]].send_json(data)  

    print(count)
    print("Done")
    
def generate_data_hrw_hashing(servers):
    print("Starting...")
    producers = create_clients(servers)
    for num in range(10000):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        node=hrw_select_node(data["key"],servers)
        print(f"HRW hashing :: Sending data:{data} to server {servers[node]}")
        producers[servers[node]].send_json(data)    
    print("Done")

    
    
if __name__ == "__main__":
    servers = []
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")
        
    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        servers.append(f'tcp://127.0.0.1:{server_port}')
        
    print("Servers:", servers)
    generate_data_round_robin(servers)
    generate_data_consistent_hashing(servers)
    generate_data_hrw_hashing(servers)
    
