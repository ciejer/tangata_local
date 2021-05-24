from flask import (Flask, render_template, request)
from flask_socketio import SocketIO, send
import os

from yaml import dump_all
import tangata_api
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description='Serve editable data catalog for dbt_')
parser.add_argument('dbtpath', help='../absolute/or/relative/path/to/dbt/project/folder/')

args = parser.parse_args()

dbtpath = args.dbtpath
app = Flask("__main__", template_folder='./build/', static_folder='./build/static/')
socketio = SocketIO(app)

def sendToast (message, type):
    print("Attempting Send Toast")
    socketio.emit('toast', {"message": message, "type": type})

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/v1/model_search/<searchString>")
def serve_search(searchString):
    print("search received")
    tangata_api.setDBTPath(dbtpath)
    return tangata_api.searchModels(searchString)

@app.route("/api/v1/models/<nodeID>")
def get_model(nodeID):
    print("get model")
    tangata_api.setDBTPath(dbtpath)
    return tangata_api.get_model(nodeID)

@app.route("/api/v1/update_metadata", methods=['POST'])
def update_metadata():
    print("post metadata update")
    tangata_api.setDBTPath(dbtpath)
    return tangata_api.update_metadata(request.json)

@app.route("/api/v1/reload_dbt", methods=['POST'])
def reload_dbt():
    print("post reload dbt")
    tangata_api.setDBTPath(dbtpath)
    return tangata_api.reload_dbt(sendToast)

if __name__ == '__main__':
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true": #On first run - debug mode triggers reruns if this isn't here
        tangata_api.setDBTPath(dbtpath)
        tangata_api.reload_dbt(sendToast)
    socketio.run(app, port=8080, debug=True)