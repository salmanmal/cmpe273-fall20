import zmq
import time
import sys
import consul
from cluster_manager import ClusterManger


def generate_data_consistent_hashing(servers):
    cm=ClusterManger()
    _=input("enter")
    requests=[
        {"op": "PUT","key": "key1","value": "value1"},
        {"op": "PUT","key": "key2","value": "value2"},
        {"op": "PUT","key": "key3","value": "value3"},
        {"op": "PUT","key": "key4","value": "value4"},
        {"op": "GET_ONE","key": "key1"},
        {"op": "GET_DISTRIBUTION"},
        {"op":"REMOVE_NODE"},
        {"op":"ADD_NODE"},
        ]

    for i in range(5,20000):
        request={"op":"PUT","key":f"key-{i}","value":f"value-{i}"}
        cm.put(request)

    for i in range(len(requests)):
        curr_req=requests[i]
        print(f"======================== Request {i+1} =================================")
        _=input(curr_req)
        if curr_req["op"]=="PUT":
            print("PUT")
            cm.put(curr_req)

        elif curr_req["op"]=="GET_ONE":
            print("GET ONE")
            _=cm.get(curr_req["key"])

        elif curr_req["op"]=="GET_ALL":
            print("GET ALL")
            _=cm.get_all()

        elif curr_req["op"]=="REMOVE_NODE":
            print("Remove Node")
            cm.remove_random_node()

        elif curr_req["op"]=="ADD_NODE":
            print("Add Node")
            cm.add_node()
        elif curr_req["op"]=="GET_DISTRIBUTION":
            print("Current Distribution")
            print(cm.getDistribution())        

    print("======================= Final Distribution ==================================")                
    print(cm.getDistribution())
    print("Done")

if __name__ == "__main__":
    servers = []
    if len(sys.argv) > 1:
        servers = sys.argv[1:]
    generate_data_consistent_hashing(servers)
    # if len(servers)>0:
        
    #     generate_data_consistent_hashing(servers)
    # else:
    #     print("please provide intial server list")
    pass


# python client.py tcp://127.0.0.1:2000 tcp://127.0.0.1:2001 tcp://127.0.0.1:2002