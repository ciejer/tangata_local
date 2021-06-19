from flask import (Flask, render_template, request)
from flask_socketio import SocketIO, send
from apscheduler.schedulers.background import BackgroundScheduler
import os

from yaml import dump_all
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description='Serve editable data catalog for dbt_')
parser.add_argument('--skip-initial-compile', action="store_true", help='Skip DBT Docs Compile on Launch')
parser.add_argument('--disable-recompile', action="store_true", help='Disable periodic recompiling of docs')

args = parser.parse_args()

skipDBTCompile = args.skip_initial_compile
disableRecompile = args.disable_recompile

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
    #     return tangata_api.searchModels(searchString)

    @app.route("/api/v1/model_search/<searchString>")
    def serve_search2(searchString):
        # search received
        return tangata_api.searchModels2(searchString)

    @app.route("/api/v1/model_tree")
    def model_tree():
        return tangata_api.get_model_tree()

    @app.route("/api/v1/db_tree")
    def db_tree():
        return tangata_api.get_db_tree()

    @app.route("/api/v1/models/<nodeID>")
    def get_model(nodeID):
        # get model
        return tangata_api.get_model(nodeID)

    @app.route("/api/v1/update_metadata", methods=['POST'])
    def update_metadata():
        # post metadata update
        updateResult = tangata_api.update_metadata(request.json, sendToast)
        return updateResult

    @app.route("/api/v1/reload_dbt", methods=['POST'])
    def reload_dbt():
        # post reload dbt
        return tangata_api.reload_dbt(sendToast)

    @app.route('/<path:path>')
    def catch_all(path):
        return render_template("index.html")
        
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true": #On first run - debug mode triggers reruns if this isn't here
        tangata_api.setSkipDBTCompile(skipDBTCompile)
        tangata_api.setDisableRecompile(disableRecompile)
        tangata_api.reload_dbt(sendToast)
        print("Tāngata now served on http://localhost:8080")
        if disableRecompile == False:
            def run_check_and_reload():
                tangata_api.check_and_reload(sendToast)
            scheduler = BackgroundScheduler(standalone=True)
            job = scheduler.add_job(run_check_and_reload, 'interval', minutes=5)
            try:
                scheduler.start()
            except (KeyboardInterrupt):
                print('Terminating Scheduler...')
    socketio.run(app, port=8080) #, debug=True)

