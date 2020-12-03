import zmq
import sys
import consul

# Register node to the cluster
def register_service(service_name,port):
    c=consul.Consul()
    c.agent.service.register(service_name,address="127.0.0.1",port=port)
    print(f"Registered :: Service {service_name} registered")


# Deregister node from the cluster
def deregister_service(service_name):
    c=consul.Consul()
    c.agent.service.deregister(service_name)
    print()
    print(f"Deregistered :: Service {service_name} deregistered")

# Helper function
# Converts dictonary of key value to array of objects
def get_all_data_formatted(data):
    response={}
    collection=[]
    print(data)
    for key,value in data.items():
        collection.append({"key":key,"value":data[key]})

    response["collection"]=collection
    return response
    
# Main function
def server(service_name,port):

    # Create channel to receive message from client
    context = zmq.Context()
    consumer = context.socket(zmq.PULL)
    consumer.bind(f"tcp://127.0.0.1:{port}")
    
    # Create channel to send message to client
    client = context.socket(zmq.PUSH)
    client.connect("tcp://127.0.0.1:4000")


    # data stored in the node
    data={}
    while True:
        # waits for the message from client
        raw = consumer.recv_json()
        print("=========================================================")

        if raw['op']=="PUT":
            # Request to store the key, value
            key, value = raw['key'], raw['value']  
            print(f"Saved Data Server_port={port}:key={key},value={value}")
            data[key]=value  
        elif raw['op']=="GET_ONE":
            # Request to get the value using key
            k = raw['key']
            response={}
            if k in data:
                response["key"]=k
                response["value"]=data[k]
            else:
                response["error"]=f"key = {k} is not in database"
            client.send_json(response)
            print(f"Return key and Data Server_port={port}:key={k},value={data[k]}")
        elif raw['op']=="GET_ALL":
            # Request to get all the data
            response=get_all_data_formatted(data)
            client.send_json(response)
            print(f"Return all data{response}")

        elif raw['op']=="GET_ALL_AND_CLEAR":
            # Request to get all the data and reset the data
            # used in rebalancing the cluster after remove or add node
            response=get_all_data_formatted(data)
            client.send_json(response)
            data={}
            print(f"Return all data{response}")
        


if __name__ == "__main__":
    port=2000
    
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    # create service name to register in consul
    service_name=f"datanode#{port}"

    # register node in consul
    register_service(service_name, port)
    try:
        print(f"Starting a server at:{port}...")
        server(service_name,port)
    except KeyboardInterrupt:
        # if process is stopped than remove the membership from consul
        deregister_service(service_name)

# python consumer.py
# python consumer.py 2002
# python consumer.py 2001