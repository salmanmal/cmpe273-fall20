from flask import Flask, request
import flask_monitoringdashboard as dashboard
from sqlitedict import SqliteDict
import random
import string


app = Flask(__name__)
dashboard.bind(app)

sqlite_db_path='./bookmark_db.sqlite'

# using SqliteDict as context manager works too (RECOMMENDED)

def get_random_alpha_numeric(length=10):
    letters = string.ascii_lowercase
    letters+="0123456789"
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

stats={}
stats_key_name="stats"

with SqliteDict(sqlite_db_path) as bookmarkDict:
    if stats_key_name in bookmarkDict:
        stats=bookmarkDict[stats_key_name]
    else:
        bookmarkDict[stats_key_name]=stats
        bookmarkDict.commit()

print(stats)
@app.route('/')
def hello():
    return "Hello world"

@app.route('/api/bookmarks',methods=['POST'])
def createBookmark():
    request_data=request.json
    
    response={}
    http_status=500

    url_key="url"
    if url_key in request_data:
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

    

@app.route('/api/bookmarks/<Id>',methods=['GET'])
def getBookmark(Id):
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
            response={"success":False,"message":f"Bookmark with {Id} not found in system!"}
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
            response={"success":False,"message":f"Bookmark with {Id} not found in system!"}
            http_status=404

    return response,http_status