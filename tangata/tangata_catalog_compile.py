import io
import os
import hashlib
import json
import git
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.filedb.filestore import RamStorage
from whoosh.analysis import StandardAnalyzer, NgramFilter

def replaceNoneHandler(data, replaceFrom, replaceTo):
    if(data):
        return data.replace(replaceFrom, replaceTo)
    else:
        return data

def populateFullCatalogNode(node, nodeOrSource, catalog, manifest):
    catalogNode = catalog[nodeOrSource+"s"][node['unique_id']]
    manifestNode = manifest[nodeOrSource+"s"][node['unique_id']]
    # print(catalogNode)
    tempFullCatalogNode = {
        "name": str.lower(catalogNode['metadata']['name']),
        "nodeID": node['unique_id'],
        "type": catalogNode['metadata']['type'],
        "database": str.lower(manifestNode['database']),
        "schema": str.lower(manifestNode['schema']),
        "description": manifestNode['description'],
        "owner": catalogNode['metadata']['owner'],
        "path": replaceNoneHandler(manifestNode['path'], '\\', '/'),
        "enabled": manifestNode['config']['enabled'],
        "materialization": manifestNode['config']['materialized'] if 'config' in manifestNode.keys() and 'materialized' in manifestNode['config'].keys() else None,
        "post_hook": manifestNode['config']["post-hook"] if 'config' in manifestNode.keys() and 'post-hook' in manifestNode['config'].keys() else None,
        "pre_hook": manifestNode['config']["pre-hook"] if 'config' in manifestNode.keys() and 'pre-hook' in manifestNode['config'].keys() else None,
        "tags": manifestNode['tags'],
        "depends_on": manifestNode['depends_on'] if 'depends_on' in manifestNode.keys() else None,
        "raw_sql": manifestNode['raw_sql'] if 'raw_sql' in manifestNode.keys() else None,
        "compiled_sql": manifestNode['compiled_sql'] if 'compiled_sql' in manifestNode.keys() else None,
        "model_type": nodeOrSource,
        "bytes_stored": catalogNode['stats']['bytes']['value'] if 'stats' in catalogNode.keys() and 'bytes' in catalogNode['stats'].keys() else None,
        "last_modified": catalogNode['stats']['last_modified']['value'] if 'stats' in catalogNode.keys() and 'last_modified' in catalogNode['stats'].keys() else None,
        "row_count": catalogNode['stats']['row_count']['value'] if 'stats' in catalogNode.keys() and 'row_count' in catalogNode['stats'].keys() else None,
        "yaml_path": replaceNoneHandler(manifestNode['patch_path'], '\\', '/'),
        "model_path": replaceNoneHandler(manifestNode['original_file_path'], '\\', '/'),
        "columns": {},
        "referenced_by": [],
        "lineage": [],
        "all_contributors": [],
        "all_commits": []
    }
    for column in catalogNode['columns'].items():
        manifestColumnNode = manifestNode['columns'][column[0]] if column[0] in manifestNode['columns'] else None
        catalogColumnNode = column[1]
        tempFullCatalogNode['columns'][str.lower(catalogColumnNode['name'])] = {
            "name": str.lower(catalogColumnNode['name']),
            "type": catalogColumnNode['type'],
            "description": manifestColumnNode['description'] if manifestColumnNode is not None and 'description' in manifestColumnNode.keys() else None,
            "tests": []
        }
    return tempFullCatalogNode


def compileCatalogNodes():
    catalogJSONRead = open("target/catalog.json", "r")
    catalog = json.load(catalogJSONRead)
    manifestJSONRead = open("target/manifest.json", "r")
    manifest = json.load(manifestJSONRead)
    tempCatalogNodes = {}
    for key in catalog['nodes'].keys():
        tempCatalogNodes[key] = populateFullCatalogNode(catalog['nodes'][key], "node", catalog, manifest)
    for key in catalog['sources'].keys():
        tempCatalogNodes[key] = populateFullCatalogNode(catalog['sources'][key], "source", catalog, manifest)
    for key in manifest['nodes'].keys():
        value = manifest['nodes'][key]
        if value['resource_type'] == "test":
            if 'depends_on' in value.keys() and 'nodes' in value['depends_on'].keys()  and len(value['depends_on']['nodes']) == 1 and 'column_name' in value.keys() and value['column_name'] is not None and len(value['column_name']) > 0: #schema test, not a relationship
                tempCatalogNodes[value['depends_on']['nodes'][0]]['columns'][str.lower(value['column_name'])]['tests'].append({"type": value['test_metadata']['name'],"severity": value['config']['severity']})
            elif 'test_metadata' in value.keys() and value['test_metadata'] and 'name' in value['test_metadata'].keys() and value['test_metadata']['name'] == "relationships": #relationship test
                catalogNodes = list(filter(lambda d: d['name'] == value['test_metadata']['kwargs']['model'].split('\'')[1], tempCatalogNodes.values()))
                if len(catalogNodes) > 0:
                    catalogNode = catalogNodes[0]
                else:
                    catalogNode = {}
                if len(catalogNode.keys()) > 0:
                    tempCatalogNodes[catalogNode['nodeID']]['columns'][str.lower(value['column_name'])]['tests'].append({"type": value['test_metadata']['name'],"severity": value['config']['severity'], "related_model": value['test_metadata']['kwargs']['to'].split('\'')[1], "related_field": str.lower(value['test_metadata']['kwargs']['field'])})
    for key, value in tempCatalogNodes.items():
        if 'depends_on' in value.keys() and value['depends_on'] is not None and 'nodes' in value['depends_on'].keys() and value['depends_on']['nodes'] is not None and len(value['depends_on']['nodes']) > 0:
            for nodeAncestor in value['depends_on']['nodes']:
                if nodeAncestor in tempCatalogNodes.keys():
                    tempCatalogNodes[nodeAncestor]['referenced_by'].append(value['nodeID'])
    return tempCatalogNodes


def compileSearchIndex(catalogToIndex):
    tempCatalogIndex = []
    for key, value in catalogToIndex.items():
        tempCatalogIndex.append({"searchable": value['name'], "nodeID": value['nodeID'], "modelName": value['name'], "modelDescription": value['description'], "type": "model_name"})
        if 'description' in value.keys() and value['description'] is not None and len(value['description']) > 0:
            tempCatalogIndex.append({"searchable": value['description'], "modelName": value['name'], "nodeID": value['nodeID'], "modelDescription": value['description'], "type": "model_description"})
        for columnKey, columnValue in value['columns'].items():
            tempCatalogIndex.append({"searchable": columnKey, "columnName": columnKey, "modelName": value['name'], "nodeID": value['nodeID'], "modelDescription": value['description'], "type": "column_name"})
            if 'description' in columnValue.keys() and columnValue['description'] is not None and len(columnValue['description']) > 0:
                tempCatalogIndex.append({"searchable": columnValue['description'], "columnName":columnKey, "modelName": value['name'], "nodeID": value['nodeID'], "modelDescription": value['description'], "type": "column_description"})
        for tagValue in value['tags']:
            if len(tagValue) > 0:
                tempCatalogIndex.append({"searchable": tagValue, "columnName": tagValue, "modelName": value['name'], "nodeID": value['nodeID'], "modelDescription": value['description'], "type": "tag_name"})
    return tempCatalogIndex

def compileSearchIndex2(catalogToIndex):
    ngram_analyzer = StandardAnalyzer() | NgramFilter(minsize=2, maxsize=4)
    schema = Schema(
        nodeID=ID(stored=True, analyzer=ngram_analyzer),
        name=ID(stored=True, field_boost=2.0, analyzer=ngram_analyzer),
        description=TEXT(stored=True, field_boost=1.0),
        tag=KEYWORD(stored=True, commas=True),
        column=KEYWORD(stored=True, commas=True)
    )
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    for doc in catalogToIndex.values():
        print(doc["name"])
        writer.add_document(
            nodeID=doc["nodeID"],
            name=doc["name"],
            description=doc["description"],
            tag=",".join(doc["tags"]),
            column=",".join(doc["columns"].keys())
        )
    writer.commit()
    return ix


def getModelLineage(fullCatalog):

    def modelLineage(currentModel):
        tempLineage = []
        def recurseForwardLineage(currentRecursedModel):
            if currentRecursedModel is not None and 'referenced_by' in currentRecursedModel:
                for refValue in currentRecursedModel['referenced_by']:
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == currentRecursedModel['nodeID']+'_'+refValue]) == 0:
                        tempLineage.append({ "id": currentRecursedModel['nodeID']+"_"+refValue, "source": currentRecursedModel['nodeID'], "target": refValue, "animated": True })
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == refValue]) == 0:
                        tempLineage.append({ "id": refValue, "data": { "label": refValue.split(".")[-1].replace('_', '_\u200B') }, "className": "lineage_"+refValue.split(".")[0]+"_node", "connectable": False})
                    recurseForwardLineage(fullCatalog[refValue])
        def recurseBackLineage(currentRecursedModel):
            if currentRecursedModel is not None and 'depends_on' in currentRecursedModel and currentRecursedModel['depends_on'] is not None and 'nodes' in currentRecursedModel['depends_on']:
                for refValue in currentRecursedModel['depends_on']['nodes']:
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == currentRecursedModel['nodeID']+'_'+refValue]) == 0:
                        tempLineage.append({ "id": currentRecursedModel['nodeID']+"_"+refValue, "target": currentRecursedModel['nodeID'], "source": refValue, "animated": True })
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == refValue]) == 0:
                        tempLineage.append({ "id": refValue, "data": { "label": refValue.split(".")[-1].replace('_', '_\u200B') }, "className": "lineage_"+refValue.split(".")[0]+"_node", "connectable": False})
                    if refValue in fullCatalog.keys():
                        recurseBackLineage(fullCatalog[refValue])


        recurseBackLineage(currentModel)
        tempLineage.append({ "id": currentModel['nodeID'], "style": {"borderColor": "tomato","borderWidth": "2px"}, "className": "lineage_"+currentModel['nodeID'].split(".")[0]+"_node", "connectable": False, "data": { "label": currentModel['name'].replace("_", '_\u200B') }})
        recurseForwardLineage(currentModel)
        return tempLineage

    for catalogNode in fullCatalog.values():
        if catalogNode['model_type'] in ['node', 'source']:
            catalogNode['lineage'] = modelLineage(catalogNode)

def checkGitChanges():

    def md5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    repo = git.Repo(os.getcwd())
    currentDiff = repo.index.diff(None)
    changedFiles = []
    for thisItem in currentDiff:
        changedFiles.append({"path": thisItem.a_path, "hash": str(md5(thisItem.a_path))})
    for thisItem in repo.untracked_files:
        changedFiles.append({"path": thisItem, "hash": str(md5(thisItem))})
    # print(changedFiles)
    return changedFiles

def getGitHistory(fullCatalog):
    def prettyRelativeDate(start_date):
        rd = relativedelta(dt.today(), start_date.replace(tzinfo=None))
        years = f'{rd.years} years, ' if rd.years > 0 else ''
        months = f'{rd.months} months, ' if rd.months > 0 else ''
        days = f'{rd.days} days' if rd.days > 0 else ''
        return f'{years}{months}{days}'
    def gitLog():
        filesList = {}
        repo = git.Repo(os.getcwd())
        commitBaseURL = repo.remotes.origin.url.replace(".git","") + "/commit/"
        if "@" in commitBaseURL:
            commitBaseURL = commitBaseURL.split("@")[1].replace(":","/")
        commitBaseURL = "http://" + commitBaseURL
        git_bin = repo.git
        git_log = git_bin.execute(['git', 'log', '--numstat', '--pretty=format:"\t\t\t%H\t%h\t%at\t%aN\t%s"'])
        git_log[:80]
        commits_raw = io.StringIO(git_log)
        hash = ''
        abbrevHash = ''
        subject = ''
        authorName = ''
        authorDateRel = ''
        authorDate = ''
        while True:
            fullLine = commits_raw.readline()
            if fullLine == '': break
            lineTabs = fullLine.split("\t")
            if (lineTabs[0] == '' or lineTabs[0] == '"') and lineTabs[1] == '':
                hash = lineTabs[3]
                abbrevHash = lineTabs[4]
                subject = lineTabs[7]
                authorName = lineTabs[6]
                authorDate = dt.fromtimestamp(int(lineTabs[5]))
                authorDateRel = prettyRelativeDate(authorDate)+' ago'
                if len(authorDateRel) < 5:
                    authorDateRel = "Today"
                print(authorDate)
            elif len(lineTabs) == 3:
                thisFile = lineTabs[2].rstrip("\n")
                if thisFile not in filesList:
                    filesList[thisFile] = {"all_commits":[]}
                filesList[thisFile]['all_commits'].append({
                    "hash": hash,
                    "originURL": commitBaseURL + hash,
                    "abbrevHash": abbrevHash,
                    "subject": subject.rstrip("\n\""),
                    "authorName": authorName,
                    "authorDateRel": authorDateRel,
                    "authorDate": authorDate.strftime("%Y-%m-%d %H:%M:%S %z")
                })
        for thisFile in filesList:
            all_contributors = []
            for thisCommit in filesList[thisFile]['all_commits']:
                all_contributors.append(thisCommit['authorName'])
            filesList[thisFile]['created_by'] = filesList[thisFile]['all_commits'][-1]['authorName']
            filesList[thisFile]['created_date'] = filesList[thisFile]['all_commits'][-1]['authorDate']
            filesList[thisFile]['created_relative_date'] = filesList[thisFile]['all_commits'][-1]['authorDateRel']
            filesList[thisFile]['all_contributors'] = list(dict.fromkeys(all_contributors))
        return filesList

    
    fullGitLog = gitLog()
    for catalogNode in fullCatalog.values():
        if catalogNode['model_path'] in fullGitLog.keys():
            eachGitLog = fullGitLog[catalogNode['model_path']]
            catalogNode['created_by'] = eachGitLog['created_by']
            catalogNode['created_date'] = eachGitLog['created_date']
            catalogNode['created_relative_date'] = eachGitLog['created_relative_date']
            catalogNode['all_contributors'] = eachGitLog['all_contributors']
            catalogNode['all_commits'] = eachGitLog['all_commits']
