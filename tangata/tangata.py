from flask import (Flask, render_template, request)
from flask_socketio import SocketIO, send
import os

from yaml import dump_all
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description='Serve editable data catalog for dbt_')
parser.add_argument('--skipcompile', action="store_true", help='Skip DBT Docs Compile')

args = parser.parse_args()

dbtpath = ".\\"
skipDBTCompile = args.skipcompile

def tangata():
    app = Flask(__name__, instance_relative_config=True)
    socketio = SocketIO(app)

    from tangata import tangata_api
    def sendToast (message, type):
        # Attempting Send Toast
        socketio.emit('toast', {"message": message, "type": type})

    @app.route("/")
    def home():
        return render_template("index.html")

    # @app.route("/api/v1/model_search/<searchString>")
    # def serve_search(searchString):
    #     # search received
    #     tangata_api.setDBTPath(dbtpath)
    #     return tangata_api.searchModels(searchString)

    @app.route("/api/v1/model_search/<searchString>")
    def serve_search2(searchString):
        # search received
        tangata_api.setDBTPath(dbtpath)
        return tangata_api.searchModels2(searchString)

    @app.route("/api/v1/model_tree")
    def model_tree():
        tangata_api.setDBTPath(dbtpath)
        return tangata_api.get_model_tree()

    @app.route("/api/v1/models/<nodeID>")
    def get_model(nodeID):
        # get model
        tangata_api.setDBTPath(dbtpath)
        return tangata_api.get_model(nodeID)

    @app.route("/api/v1/update_metadata", methods=['POST'])
    def update_metadata():
        # post metadata update
        tangata_api.setDBTPath(dbtpath)
        return tangata_api.update_metadata(request.json)

    @app.route("/api/v1/reload_dbt", methods=['POST'])
    def reload_dbt():
        # post reload dbt
        tangata_api.setDBTPath(dbtpath)
        return tangata_api.reload_dbt(sendToast)

    @app.route('/<path:path>')
    def catch_all(path):
        return render_template("index.html")
        
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true": #On first run - debug mode triggers reruns if this isn't here
        tangata_api.setDBTPath(dbtpath)
        tangata_api.setSkipDBTCompile(skipDBTCompile)
        tangata_api.reload_dbt(sendToast)
    socketio.run(app, port=8080) #, debug=True)
