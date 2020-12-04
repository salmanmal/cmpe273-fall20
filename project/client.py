import zmq
import time
import sys
import consul
from cluster_manager import ClusterManger


def generate_data_consistent_hashing(servers):
    cm=ClusterManger()
    _=input("enter")

    # Iterate over to put the keys
    for i in range(5,100):
        request={"op":"PUT","key":f"key-{i}","value":f"value-{i}"}
        cm.put(request)



    # Process following requests one by one
    requests=[
        {"op": "PUT","key": "key-1","value": "value-1"},
        {"op": "PUT","key": "key-2","value": "value-2"},
        {"op": "PUT","key": "key-3","value": "value-3"},
        {"op": "PUT","key": "key-4","value": "value-4"},
        {"op": "GET_ONE","key": "key-1"},
        {"op": "GET_DISTRIBUTION"},
        {"op":"REMOVE_NODE"},
        {"op":"ADD_NODE"},
        {"op": "GET_ONE","key": "key-1"},
        ]

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
