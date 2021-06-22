import os
import json
from git import refresh
# from yaml import load, dump
# try:
#     from yaml import CLoader as Loader, CDumper as Dumper
# except ImportError:
#     from yaml import Loader, Dumper
from ruamel.yaml import YAML
    # yaml = YAML()
    # yaml.indent(mapping=2, sequence=4, offset=2)
    # yaml.load(catalogJSONRead)
import re
from tangata import tangata_catalog_compile
from functools import reduce
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.filedb.filestore import RamStorage

# class CustomDumper(Dumper):
#     #Super neat hack to preserve the mapping key order. See https://stackoverflow.com/a/52621703/1497385
#     def represent_dict_preserve_order(self, data):
#         return self.represent_dict(data.items())
#     # def increase_indent(self, flow=False, indentless=False):
#     #     return super(MyDumper, self).increase_indent(flow, False)    

# CustomDumper.add_representer(dict, CustomDumper.represent_dict_preserve_order)

skipDBTCompile = False
disableRecompile = False
lastGitIndex = {}

def setSkipDBTCompile(newSkipDBTCompile):
    global skipDBTCompile
    skipDBTCompile = newSkipDBTCompile

def setDisableRecompile(newDisableRecompile):
    global disableRecompile
    disableRecompile = newDisableRecompile

catalogPath = "./tangata_catalog.json"
catalogIndexPath = "./tangata_catalog_index.json"
catalog = {}
catalogIndex = []
catalogWhooshIndex = {}

        with open("target/tangata_catalog.json", "r") as cat:
            catalog = json.load(cat)
        with open("target/tangata_catalog_index.json", "r") as catIndex:
            catalogIndex = json.load(catIndex)


def refreshMetadata(sendToast):
    global catalog
    global catalogIndex
    global catalogWhooshIndex
    print("Refreshing DBT Catalog...")
    if not os.path.isfile("target/catalog.json"):
        print("DBT generated docs not available..")
        sendToast("catalog.json not found in folder.", "error")
        return
    print("Compiling Catalog Nodes...")
    assemblingFullCatalog = tangata_catalog_compile.compileCatalogNodes()
    print("Compiling Catalog Index...")
    assemblingCatalogIndex = tangata_catalog_compile.compileSearchIndex(assemblingFullCatalog)
    catalogWhooshIndex = tangata_catalog_compile.compileSearchIndex2(assemblingFullCatalog)
    print("Assembling Lineage...")
    tangata_catalog_compile.getModelLineage(assemblingFullCatalog)
    print("Assembling Git History...")
    tangata_catalog_compile.getGitHistory(assemblingFullCatalog)
    print("Storing Compiled Catalog...")
    catalog = assemblingFullCatalog
    catalogIndex = assemblingCatalogIndex
    sendToast("Metadata has been refreshed successfully.", "success")
    with open("target/tangata_catalog.json", "w") as cat:
        json.dump(catalog, cat)
    with open("target/tangata_catalog_index.json", "w") as catIndex:
        json.dump(catalogIndex, catIndex)


def searchModels(searchString):
    def modelCompare(inputItem, searchString):
        isModel = 1
        if inputItem['type'] == "model_name":
            isModel = 0
        hasDescription = 1
        if len(inputItem['modelDescription']) > 0:
            hasDescription = 0
        searchStringLengthDiff = abs(len(inputItem['searchable'])-len(searchString))
        # print((inputItem['searchable'], inputItem['type'], inputItem['modelName'], searchStringLengthDiff, isModel, hasDescription))
        return (searchStringLengthDiff, isModel, hasDescription)
    if len(searchString)>3:
        print(searchString)
        denied_metrics = [re.compile(searchString), re.compile("c$")]
        matches = [model for model in catalogIndex
            if re.compile(searchString).search(model['searchable'])]
        if len(matches) > 0:

            # print(type(matches))
            # print(matches)
            matches = sorted(matches, key = lambda k: (modelCompare(k, searchString)))
            results = json.dumps(matches)
            # print(json.dumps(matches))
            return '{"results": ' + results + ',"searchString":"' + searchString + '"}'
        else:
            return '{"results": [],"searchString":"' + searchString + '"}'
    else:
        return '{"results": [],"searchString":"' + searchString + '"}'

def searchModels2(searchString):
    with catalogWhooshIndex.searcher() as searcher:
        query = MultifieldParser(["nodeID", "name","description","tag","column"], schema=catalogWhooshIndex.schema)
        parsedquery = query.parse(searchString)
        searchMatches = searcher.search(parsedquery)
        matches = [dict(hit) for hit in searchMatches]
        foundModels = []
        if len(matches) > 0:
            for thisMatch in matches:
                foundModels.append({
                    "nodeID": thisMatch['nodeID'],
                    "modelName": catalog[thisMatch['nodeID']]['name'],
                    "modelDescription": catalog[thisMatch['nodeID']]['description'],
                    "modelTags": catalog[thisMatch['nodeID']]['tags']
                })
        results = json.dumps(foundModels)
        return '{"results": ' + results + ',"searchString":"' + searchString + '"}'
    
def get_model_tree():
    def filter_model_name(indexRecord):
        return indexRecord['type'] == "model_name"
    def split_models(res, cur):
        splitVal = reduce(lambda res, cur: {cur: res}, reversed(cur["nodeID"].split(".")), {})
        res.append(splitVal)
        return res
    def merge_models(res, cur):
        return merge(res, cur)

    all_models = list(filter(filter_model_name, catalogIndex))
    split_models = reduce(split_models, all_models, [])
    resultObject = reduce(merge_models, split_models, {})
    return resultObject
    
def get_db_tree():
    def get_db_keys(item):
        db_keys = ["database", "schema", "name", "nodeID"]
        return {key: catalog[item][key] for key in db_keys}

    all_models = list(map(get_db_keys, catalog))
    return {'db_models': all_models}

def get_model(nodeID):
    result = catalog[nodeID]
    return result

def findOrCreateMetadataYML(yaml_path, model_path, model_name, source_schema, model_or_source):
    def useSchemaYML():
        # using useSchemaYML
        def createNewYML(schemaPath, modelName, sourceSchema):
            # createNewYML
            if(model_or_source=='model'):
                # createNewYML - model
                newYAML = {"version": 2,"models": [{"name": modelName}]}
            else:
                # createNewYML - source
                newYAML = {"version": 2,"sources": [{"name": source_schema,"tables": [{"name": modelName}]}]}
            newYamlWrite = open(schemaPath, "w")
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.dump(newYAML, newYamlWrite)
            return schemaPath
        path = '' + model_path
        print(path)
        print(path.rindex('/'))
        print(path[0:path.rindex('/')])
        path = path[0:path.rindex('/')]
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            # useSchemaYML - directory doesn't exist
            os.makedirs(directory)
        schemaPath = path+'/schema.yml'
        try:
            if os.path.isfile(schemaPath):
                # useSchemaYML - schemaPath exists
                schemaPathRead = open(schemaPath, "r")
                yaml = YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                currentSchemaYML = yaml.load(schemaPathRead)
                if model_or_source == 'model':
                    # useSchemaYML - is model
                    if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                        # useSchemaYML - found model in file
                        return schemaPath
                    else:
                        print('useSchemaYML - pushing model')
                        currentSchemaYML['models'].append({"name": model_name})
                        schemaPathWrite = open(schemaPath, "w")
                        yaml.dump(currentSchemaYML, schemaPathWrite)
                        schemaPathWrite.close()
                        print('useSchemaYML - pushed model')
                else:
                    # useSchemaYML - source
                    if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) > 0 and len(list(filter(lambda d: d['name'] == model_name, list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))[0]['tables']))) > 0:
                        # useSchemaYML - found source in file
                        return schemaPath
                    else:
                        # useSchemaYML - did not find source in file
                        if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) == 0: #add source and table
                            # pushing source and table
                            currentSchemaYML['sources'].append({"name": source_schema,"tables": [{"name": model_name}]})
                        else: #add just source table
                            # pushing just table
                            list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))['tables'].append({"name": model_name})
                        schemaPathWrite = open(schemaPath, "w")
                        yaml.dump(currentSchemaYML, schemaPathWrite)
                        schemaPathWrite.close()
                return schemaPath
            else:
                return createNewYML(schemaPath, model_name, source_schema)
        except:
            return createNewYML(schemaPath, model_name, source_schema)
    print(source_schema)
    print(model_name)
    if model_or_source == 'source':
        # is source
        path = '' + model_path
        print(path)
        try:
            if os.path.isfile(path):
                # first try path is file
                pathRead = open(path, "r")
                yaml = YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                currentSchemaYML = yaml.load(pathRead)
                pathRead.close()
                # opened yaml
                if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) > 0 and len(list(filter(lambda d: d['name'] == model_name, list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))[0]['tables']))) > 0:
                    # found source on first try
                    return path
                else:
                    # did not source on first try
                    if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) == 0: #add source and table
                        # pushing source and table
                        currentSchemaYML['sources'].append({"name": source_schema,"tables": [{"name": model_name}]})
                    else: #add just source table
                        # pushing just table
                        list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))['tables'].append({"name": model_name})
                    pathWrite = open(path, "w")
                    yaml.dump(currentSchemaYML, pathWrite)
                    pathWrite.close()
                return path
            else:
                return useSchemaYML()
        except:
            return useSchemaYML()
    elif yaml_path is not None and len(yaml_path) > 0:
        path = '' + yaml_path
        try:
            if os.path.isfile(path):
                pathRead = open(path, "r")
                yaml = YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                currentSchemaYML = yaml.load(pathRead)
                pathRead.close()
                if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                    # found model in file
                    return path
                else:
                    print('pushing model')
                    print(currentSchemaYML)
                    currentSchemaYML['models'].append({"name": model_name})
                    print('now pushed, list is now:')
                    print(currentSchemaYML)
                    pathWrite = open(path, "w")
                    yaml.dump(currentSchemaYML, pathWrite)
                    pathWrite.close()
                return path
            else:
                return useSchemaYML()
        except:
            return useSchemaYML()
    else:
        return useSchemaYML()

def merge(a, b, path=None):
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def update_metadata(jsonBody, sendToast):
    print(jsonBody)
    if jsonBody['updateMethod'] == 'yamlModelProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        currentSchemaYML = yaml.load(schemaPathRead)
        schemaPathRead.close()
        if jsonBody['node_id'].split(".")[0] == 'model':
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], currentSchemaYML['models']))[0]
        else:
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
        currentSchemaYMLModel[jsonBody['property_name']] = jsonBody['new_value']
        pathWrite = open(schemaYMLPath, "w")
        yaml.dump(currentSchemaYML, pathWrite)
        pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelTags':
        if jsonBody['node_id'].split(".")[0] == 'model':
            dbtProjectYMLModelPath = ['models', jsonBody['node_id'].split(".")[1]]
            splitModelPath = jsonBody['model_path'].split(".")[0].split("/")
            splitModelPath.pop(0)
            dbtProjectYMLModelPath = dbtProjectYMLModelPath + splitModelPath
            readDbtProjectYml = open(''+"dbt_project.yml", "r")
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            dbtProjectYML = yaml.load(readDbtProjectYml)
            readDbtProjectYml.close()
            jsonTags = json.loads("[\""+"\",\"".join(jsonBody['new_value'])+"\"]")
            yamlModel = dbtProjectYML
            for pathStep in dbtProjectYMLModelPath:
                if not pathStep in yamlModel:
                    yamlModel[pathStep] = {'tags': []}
                yamlModel = yamlModel[pathStep]

            yamlModel['tags'] = jsonTags
            writeDbtProjectYml = open("dbt_project.yml", "w")
            yaml.dump(dbtProjectYML, writeDbtProjectYml)
            writeDbtProjectYml.close()

        else:
            schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
            schemaPathRead = open(schemaYMLPath, "r")
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            currentSchemaYML = yaml.load(schemaPathRead)
            schemaPathRead.close()
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
            currentSchemaYMLModel['tags'] = jsonBody['new_value']
            pathWrite = open(schemaYMLPath, "w")
            yaml.dump(currentSchemaYML, pathWrite)
            pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        currentSchemaYML = yaml.load(schemaPathRead)
        schemaPathRead.close()
        if jsonBody['node_id'].split(".")[0] == 'model':
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], currentSchemaYML['models']))[0]
        else:
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
        # about to check for columns
        if 'columns' in currentSchemaYMLModel.keys():
            # columns exist
            if len(list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))) == 0:
                currentSchemaYMLModel['columns'].append({"name": jsonBody['column']})
            currentSchemaYMLModelColumn = list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))[0]
        else:
            currentSchemaYMLModel['columns'] = [{"name": jsonBody['column']}]
            currentSchemaYMLModelColumn = list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))[0]
        currentSchemaYMLModelColumn[jsonBody['property_name']] = jsonBody['new_value']
        pathWrite = open(schemaYMLPath, "w")
        yaml.dump(currentSchemaYML, pathWrite)
        pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnTest':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        currentSchemaYML = yaml.load(schemaPathRead)
        schemaPathRead.close()
        if jsonBody['node_id'].split(".")[0] == 'model':
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], currentSchemaYML['models']))[0]
        else:
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
        # about to check for columns
        if 'columns' in currentSchemaYMLModel.keys():
            # columns exist
            if len(list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))) == 0:
                currentSchemaYMLModel['columns'].append({"name": jsonBody['column']})
            currentSchemaYMLModelColumn = list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))[0]
            print(currentSchemaYMLModelColumn)
        else:
            currentSchemaYMLModel['columns'] = {"name": jsonBody['column']}
            currentSchemaYMLModelColumn = list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))[0]
        if len(jsonBody['new_value']) > 0:
            currentSchemaYMLModelColumn['tests'] = jsonBody['new_value']
        else:
            del currentSchemaYMLModelColumn['tests']
        print(currentSchemaYMLModel)
        pathWrite = open(schemaYMLPath, "w")
        yaml.dump(currentSchemaYML, pathWrite)
        pathWrite.close()
    return "success"

def reload_dbt(sendToast):
    global skipDBTCompile
    global lastGitIndex
    if skipDBTCompile:
        print("Skipping DBT Compile.")
    else:
        print("reloading dbt_...")
        dbtRunner = os.system("dbt docs generate")
        print("complete")
        print(dbtRunner)
        if dbtRunner == 0 :
            print("dbt_ update successful. Updating app catalog...")
            sendToast("dbt_ update successful.", "success")
        else:
            print("dbt_ update failed. Trying metadata compile anyway.")
    refreshMetadata(sendToast)
    skipDBTCompile = False #Set to allow compiles from button later
    lastGitIndex = tangata_catalog_compile.checkGitChanges()
    return "success"

def check_and_reload(sendToast):
    global lastGitIndex
    thisGitIndex = tangata_catalog_compile.checkGitChanges()
    if thisGitIndex != lastGitIndex:
        print("Repository changes found, running dbt")
        lastGitIndex = thisGitIndex
        reload_dbt(sendToast)