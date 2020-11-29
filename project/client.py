import zmq
import time
import sys
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

def generate_data_consistent_hashing(servers):
    print("Starting...")
    context = zmq.Context()
    consumer_response = context.socket(zmq.PULL)
    consumer_response.bind("tcp://127.0.0.1:4000")
    producers = create_clients(servers)
    ch=ConsistentHashing(servers)
    requests=[
        {"op": "PUT","key": "key1","value": "value1"},
        {"op": "PUT","key": "key2","value": "value2"},
        {"op": "PUT","key": "key3","value": "value3"},
        {"op": "PUT","key": "key4","value": "value4"},
        {"op": "GET_ONE","key": "key1"},
        {"op": "GET_ALL"},
        {"op":"REMOVE_NODE","server":"tcp://127.0.0.1:2000"},
        {"op":"ADD_NODE","server":"tcp://127.0.0.1:2002"},
        {"op": "GET_ALL"}]

    for i in range(len(requests)):
        curr_req=requests[i]
        print("=========================================================")
        if curr_req["op"]=="PUT":
            node = ch.select_node_for_put(curr_req["key"])
            producers[servers[node]].send_json(curr_req)  
            print(f"Consistent hashing :: Sending data:{curr_req} to server {servers[node]}")
        elif curr_req["op"]=="GET_ONE":
            node = ch.select_node(curr_req["key"])
            producers[servers[node]].send_json(curr_req)  
            print(f"=> Consistent hashing :: Fetching key data:{curr_req} from server {servers[node]}")
            response= consumer_response.recv_json()
            print(f"=> Consistent hashing :: Fetched key data:{response} from server {servers[node]}")

        elif curr_req["op"]=="GET_ALL":
            all_result=[]
            for server in servers:
                producers[server].send_json(curr_req)
                response= consumer_response.recv_json()
                all_result.extend(response["collection"])
            
            print(f"Consistent hashing :: All data from all server:{all_result}")

        elif curr_req["op"]=="REMOVE_NODE":
            print("distribution before removing the node:")
            print(ch.getDistribution())
            producers[curr_req["server"]].send_json({"op":"GET_ALL"})
            response=consumer_response.recv_json()
            servers.remove(curr_req["server"])
            ch.update_servers(servers)
            collection=response["collection"]

            for i in range(len(collection)):
                node = ch.select_node_for_put(collection[i]["key"])
                collection[i]["op"]="PUT"
                producers[servers[node]].send_json(collection[i])  
            
            print("distribution after the rebalancing:")
            print(ch.getDistribution())
        elif curr_req["op"]=="ADD_NODE":
            print("distribution before adding the node:")
            print(ch.getDistribution())
            all_result=[]
            
            for server in servers:
                producers[server].send_json({"op":"GET_ALL"})
                response= consumer_response.recv_json()
                all_result.extend(response["collection"])
            
            servers.append(curr_req["server"])

            context = zmq.Context()
            producer_conn = context.socket(zmq.PUSH)
            producer_conn.bind(curr_req["server"])
            producers[curr_req["server"]]=producer_conn
            
            ch.update_servers(servers)
            for i in range(len(all_result)):
                node = ch.select_node_for_put(all_result[i]["key"])
                all_result[i]["op"]="PUT"
                producers[servers[node]].send_json(all_result[i])  

            print("distribution after the rebalancing:")
            print(ch.getDistribution())

    print("=========================================================")                
    print(ch.getDistribution())
    print("Done")

if __name__ == "__main__":
    servers = []
    if len(sys.argv) > 1:
        servers=sys.argv[1:]
    
    if len(servers)>0:
        generate_data_consistent_hashing(servers)
        
    else:
        print("please provide intial server list")
    pass


# python client.py tcp://127.0.0.1:2000 tcp://127.0.0.1:2001 tcp://127.0.0.1:2002