from flask import Flask, request, send_file, request, Response, make_response
import flask_monitoringdashboard as dashboard
import random
import string
import zmq

# export FLASK_DEBUG=true
# export FLASK_APP=app.py

app = Flask(__name__)
dashboard.bind(app)

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers

# Generate Random number for new bookmarks for Id
def get_random_alpha_numeric(length=10):
    letters = string.ascii_lowercase
    letters+="0123456789"
    result_str = ''.join(random.choice(letters) for i in range(length))
    

    # check for duplicates
    with SqliteDict(sqlite_db_path) as bookmarkDict:
        if result_str in bookmarkDict:
            return get_random_alpha_numeric()
    return result_str

@app.route('/')
def ping():
    return "Server is up!"

@app.route('/api/key-value',methods=['POST'])
def createBookmark():
    request_data=request.json
    
    response={}
    http_status=500

    url_key="url"
    if url_key in request_data:
        if isUrlDuplicate(request_data[url_key]) :
            http_status=400
            response={"reason": "The given URL already existed in the system."}
        else:
            with SqliteDict(sqlite_db_path) as bookmarkDict:  # note no autocommit=True

                # Generate Alpha Numeric Unique Id
                Id=get_random_alpha_numeric()

                # Add new bookmark in dictonary
                request_data["id"]=Id
                bookmarkDict[Id] = request_data

                # Start stat and update stat in dictionary
                stats[Id]=0
                bookmarkDict[stats_key_name]=stats

                # commit changes (AUTO Commit is false )
                bookmarkDict.commit()

                # return id in response with HTTP Status code 201
                response={"id":Id}
                http_status=201            
    else:
        # Url is not available in request body
        response={"success":False,"message":"Bookmark Url is missing in request!","data":{}}
        http_status=412

    return response,http_status

@app.route('/api/key-value/<key>',methods=['GET'])
def getBookmark(key):
    response={}
    http_status=500
    with SqliteDict(sqlite_db_path) as bookmarkDict:
        if Id in bookmarkDict:
            # get response body
            response=bookmarkDict[Id]

            # Increment stats both in database and server
            # stats=bookmarkDict[stats_key_name]
            stats[Id]=stats[Id]+1
            bookmarkDict[stats_key_name]=stats

            # commit changes (AUTO Commit is false )
            bookmarkDict.commit()

            http_status=200
        else :
            response={"success":False,"message":f"Bookmark with Id {Id} not found in system!"}
            http_status=404

    return response,http_status

@app.route('/api/bookmarks/<Id>',methods=['DELETE'])
def deleteBookmark(Id):
    response={}
    http_status=500
    with SqliteDict(sqlite_db_path) as bookmarkDict:
        if Id in bookmarkDict:
            # remove bookmark from dictionary
            bookmarkDict.pop(Id,None)

            # remove key from stats
            # stats=bookmarkDict[stats_key_name]
            stats.pop(Id)
            bookmarkDict[stats_key_name]=stats
            bookmarkDict.commit()
            http_status=204
        else :
            response={"success":False,"message":f"Bookmark with Id {Id} not found in system!"}
            http_status=404

    return response,http_status

@app.route('/api/bookmarks/<Id>/qrcode',methods=['GET'])
def getQRCode(Id):
    response={}
    http_status=500
    with SqliteDict(sqlite_db_path) as bookmarkDict:
        if Id in bookmarkDict:
            # get bookmark url from dictionary
            url=bookmarkDict[Id]["url"]

            # generate qrcode for url
            qrcode=get_qrcode(url)
            response=send_file(qrcode,mimetype='image/png')
            http_status=200
        else :
            response={"success":False,"message":f"Bookmark with Id {Id} not found in system!"}
            http_status=404

    return response,http_status

@app.route('/api/bookmarks/<Id>/stats',methods=['GET'])
def getBookmarkStats(Id):
    response={}
    http_status=500
    with SqliteDict(sqlite_db_path) as bookmarkDict:
        if Id in stats:
            if int(request.headers.get('ETag',0))==stats[Id]:
                http_status=304
            else:
                response = make_response(f"{stats[Id]}")
                response.set_etag(f"{stats[Id]}")
                http_status=200
        else:
            response={"success":False,"message":f"Bookmark with Id {Id} not found in system!"}
            http_status=404
    return response,http_status
    

