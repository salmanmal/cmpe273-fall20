from flask import Flask, request, send_file, request, Response, make_response
import flask_monitoringdashboard as dashboard
import random
import string
import zmq
import consul
from cluster_manager import ClusterManger


# export FLASK_DEBUG=true
# export FLASK_APP=app.py

app = Flask(__name__)

@app.route('/')
def ping():
    return "Server is up!"

@app.route('/api/keyValue',methods=['POST'])
def addKeyValue():
    global ch
    request_data=request.json
    
    response={}
    http_status=500

    if "key" in request_data and "value" in request_data:
        ch.put(request_data)
        response={"success":False, "message":"Key value pair added successfully."}
        http_status=201
    else:
        # key and value is not available in request body
        response={"success":False,"message":"Key value pair is missing in request!","data":{}}
        http_status=412

    return response, http_status

@app.route('/api/keyValue/<key>',methods=['GET'])
def getValue(key):
    global ch
    response={}
    http_status=500

    response= ch.get(key)
    
    if "error" in response:
        response={"success":False,"message":response["error"]}
        http_status=404
    else :
        response={"success":True,"data":response}
        http_status=200

    return response,http_status

@app.route('/api/getAll',methods=['GET'])
def getAllData(Id):
    global producers, servers
    response={}
    http_status=500
    all_result=[]
    for server in servers:
        producers[server].send_json(curr_req)
        response= consumer_response.recv_json()
        all_result.extend(response["collection"])

    http_status=200
    response={"success":True,data:all_result}

    return response,http_status


@app.route('/api/removeDataNode',methods=['GET'])
def addDataNode(Id):
    global producers, servers
    response={}
    http_status=500
    all_result=[]
    for server in servers:
        producers[server].send_json(curr_req)
        response= consumer_response.recv_json()
        all_result.extend(response["collection"])

    http_status=200
    response={"success":True,data:all_result}

    return response,http_status


if __name__ == '__main__':
    try:
        global ch
        ch=ClusterManger()
        app.run(host='0.0.0.0',port=5000,debug = True)
        print("h")
        
        
    except KeyboardInterrupt:
        ch.unbind()