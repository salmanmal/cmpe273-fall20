import zmq
import sys
def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.PULL)
    consumer.connect(f"tcp://127.0.0.1:{port}")
    
    client = context.socket(zmq.PUSH)
    client.connect("tcp://127.0.0.1:4000")

    data={}
    while True:
        raw = consumer.recv_json()
        print("=========================================================")

        if raw['op']=="PUT":
            key, value = raw['key'], raw['value']  
            print(f"Saved Data Server_port={port}:key={key},value={value}")
            data[key]=value  
        elif raw['op']=="GET_ONE":
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
            response={}
            collection=[]
            print(data)
            for key,value in data.items():
                collection.append({"key":key,"value":data[key]})

            response["collection"]=collection
            client.send_json(response)
            print(f"Return all data{response}")
            
        
        # FIXME: Implement to store the key-value data.
        data[key]=value


if __name__ == "__main__":
    port=2000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])


    print(f"Starting a server at:{port}...")
    server(port)


# python consumer.py
# python consumer.py 2002
# python consumer.py 2001