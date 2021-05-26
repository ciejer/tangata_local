import io
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import git
from datetime import datetime
from dateutil.relativedelta import relativedelta

class CustomDumper(Dumper):
    #Super neat hack to preserve the mapping key order. See https://stackoverflow.com/a/52621703/1497385
    def represent_dict_preserve_order(self, data):
        return self.represent_dict(data.items())
    # def increase_indent(self, flow=False, indentless=False):
    #     return super(MyDumper, self).increase_indent(flow, False)    

CustomDumper.add_representer(dict, CustomDumper.represent_dict_preserve_order)

dbtpath = ''

def setDBTPath(newDBTPath):
    global dbtpath
    dbtpath = newDBTPath

catalogPath = "./tangata_catalog.json"
catalogIndexPath = "./tangata_catalog_index.json"

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
        "path": manifestNode['path'],
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
        "yaml_path": manifestNode['patch_path'],
        "model_path": manifestNode['original_file_path'],
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
    catalogJSONRead = open(dbtpath+"target/catalog.json", "r")
    catalog = load(catalogJSONRead, Loader=Loader)
    manifestJSONRead = open(dbtpath+"target/manifest.json", "r")
    manifest = load(manifestJSONRead, Loader=Loader)
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


def getModelLineage(fullCatalog):

    def modelLineage(currentModel):
        tempLineage = []
        def recurseForwardLineage(currentRecursedModel):
            if currentRecursedModel is not None and 'referenced_by' in currentRecursedModel:
                for refValue in currentRecursedModel['referenced_by']:
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == currentRecursedModel['nodeID']+'_'+refValue]) == 0:
                        tempLineage.append({ "id": currentRecursedModel['nodeID']+"_"+refValue, "source": currentRecursedModel['nodeID'], "target": refValue, "animated": True })
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == refValue]) == 0:
                        tempLineage.append({ "id": refValue, "data": { "label": refValue.split(".")[-1].replace('_', '_\u200B') }, "connectable": False})
                    recurseForwardLineage(fullCatalog[refValue])
        def recurseBackLineage(currentRecursedModel):
            if currentRecursedModel is not None and 'depends_on' in currentRecursedModel and currentRecursedModel['depends_on'] is not None and 'nodes' in currentRecursedModel['depends_on']:
                for refValue in currentRecursedModel['depends_on']['nodes']:
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == currentRecursedModel['nodeID']+'_'+refValue]) == 0:
                        tempLineage.append({ "id": currentRecursedModel['nodeID']+"_"+refValue, "target": currentRecursedModel['nodeID'], "source": refValue, "animated": True })
                    if len([existingRow for existingRow in tempLineage if existingRow['id'] == refValue]) == 0:
                        tempLineage.append({ "id": refValue, "data": { "label": refValue.split(".")[-1].replace('_', '_\u200B') }, "connectable": False})
                    if refValue in fullCatalog.keys():
                        recurseBackLineage(fullCatalog[refValue])


        recurseBackLineage(currentModel)
        tempLineage.append({ "id": currentModel['nodeID'], "style": {"borderColor": "tomato","borderWidth": "2px"}, "connectable": False, "data": { "label": currentModel['name'].replace("_", '_\u200B') }})
        recurseForwardLineage(currentModel)
        return tempLineage

    for catalogNode in fullCatalog.values():
        if catalogNode['model_type'] in ['node', 'source']:
            catalogNode['lineage'] = modelLineage(catalogNode)

def getGitHistory(fullCatalog):
    def prettyRelativeDate(start_date):
        rd = relativedelta(datetime.today(), start_date.replace(tzinfo=None))
        years = f'{rd.years} years, ' if rd.years > 0 else ''
        months = f'{rd.months} months, ' if rd.months > 0 else ''
        days = f'{rd.days} days' if rd.days > 0 else ''
        return f'{years}{months}{days}'
    def gitLog():
        filesList = {}
        repo = git.Repo(dbtpath)
        git_bin = repo.git
        git_log = git_bin.execute('git log --numstat --pretty=format:"\t\t\t%H\t%h\t%at\t%aN\t%s"')
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
            if lineTabs[0] == '' and lineTabs[1] == '':
                hash = lineTabs[3]
                abbrevHash = lineTabs[4]
                subject = lineTabs[7]
                authorName = lineTabs[6]
                authorDate = datetime.fromtimestamp(int(lineTabs[5]))
                authorDateRel = prettyRelativeDate(authorDate)+' ago'
                print(authorDate)
            elif len(lineTabs) == 3:
                thisFile = lineTabs[2].rstrip("\n").replace("/","\\")
                if thisFile not in filesList:
                    filesList[thisFile] = {"all_commits":[]}
                filesList[thisFile]['all_commits'].append({
                    "hash": hash,
                    "abbrevHash": abbrevHash,
                    "subject": subject.rstrip("\n"),
                    "authorName": authorName,
                    "authorDateRel": authorDateRel,
                    "authorDate": authorDate.strftime("%Y-%m-%d %H:%M:%S %z")
                })
        for thisFile in filesList:
            all_contributors = []
            for thisCommit in filesList[thisFile]['all_commits']:
                all_contributors.append(thisCommit['authorName'])
            filesList[thisFile]['created_by'] = filesList[thisFile]['all_commits'][0]['authorName']
            filesList[thisFile]['created_date'] = filesList[thisFile]['all_commits'][0]['authorDate']
            filesList[thisFile]['created_relative_date'] = filesList[thisFile]['all_commits'][0]['authorDateRel']
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
