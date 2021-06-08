import os
import json
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import re
from tangata import tangata_catalog_compile
from functools import reduce
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.filedb.filestore import RamStorage

class CustomDumper(Dumper):
    #Super neat hack to preserve the mapping key order. See https://stackoverflow.com/a/52621703/1497385
    def represent_dict_preserve_order(self, data):
        return self.represent_dict(data.items())
    # def increase_indent(self, flow=False, indentless=False):
    #     return super(MyDumper, self).increase_indent(flow, False)    

CustomDumper.add_representer(dict, CustomDumper.represent_dict_preserve_order)

dbtpath = ''
skipDBTCompile = False

def setDBTPath(newDBTPath):
    global dbtpath
    dbtpath = newDBTPath
def setSkipDBTCompile(newSkipDBTCompile):
    global skipDBTCompile
    skipDBTCompile = newSkipDBTCompile

catalogPath = "./tangata_catalog.json"
catalogIndexPath = "./tangata_catalog_index.json"
catalog = {}
catalogIndex = []
catalogWhooshIndex = {}

def refreshMetadata(sendToast):
    global catalog
    global catalogIndex
    global catalogWhooshIndex
    print("Refreshing DBT Catalog...")
    if not os.path.isfile(dbtpath + "target/catalog.json"):
        print("DBT generated docs not available..")
        sendToast("catalog.json not found in folder.", "error")
        return
    tangata_catalog_compile.setDBTPath(dbtpath)
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
    print(searchString)
    with catalogWhooshIndex.searcher() as searcher:
        query = MultifieldParser(["nodeID", "name","description","tag","column"], schema=catalogWhooshIndex.schema)
        print(query)
        parsedquery = query.parse(searchString)
        print(parsedquery)
        print(list(searcher.lexicon("column")))
        searchMatches = searcher.search(parsedquery)
        print(searchMatches[0])
        matches = [dict(hit) for hit in searchMatches]
        print(matches)
        foundModels = []
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
    resultObject = reduce(merge_models, split_models)
    return resultObject

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
            yamlToWrite = dump(newYAML, Dumper=CustomDumper)
            print(yamlToWrite)
            newYamlWrite = open(schemaPath, "w")
            newYamlWrite.write(yamlToWrite)
            return schemaPath
        path = dbtpath + model_path.replace('\\','/')
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
                currentSchemaYML = load(schemaPathRead, Loader=Loader)
                schemaPathRead.close()
                if model_or_source == 'model':
                    # useSchemaYML - is model
                    if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                        # useSchemaYML - found model in file
                        return schemaPath
                    else:
                        print('useSchemaYML - pushing model')
                        currentSchemaYML['models'].append({"name": model_name})
                        schemaPathWrite = open(schemaPath, "w")
                        schemaPathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
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
                        schemaPathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
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
        path = dbtpath + model_path.replace('\\','/')
        print(path)
        try:
            if os.path.isfile(path):
                # first try path is file
                pathRead = open(path, "r")
                currentSchemaYML = load(pathRead, Loader=Loader)
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
                    pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
                    pathWrite.close()
                return path
            else:
                return useSchemaYML()
        except:
            return useSchemaYML()
    elif yaml_path is not None and len(yaml_path) > 0:
        path = dbtpath + yaml_path.replace('\\','/')
        try:
            if os.path.isfile(path):
                pathRead = open(path, "r")
                currentSchemaYML = load(pathRead, Loader=Loader)
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
                    pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
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

def update_metadata(jsonBody):
    print(jsonBody)
    if jsonBody['updateMethod'] == 'yamlModelProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        currentSchemaYML = load(schemaPathRead, Loader=Loader)
        schemaPathRead.close()
        if jsonBody['node_id'].split(".")[0] == 'model':
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], currentSchemaYML['models']))[0]
        else:
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
        currentSchemaYMLModel[jsonBody['property_name']] = jsonBody['new_value']
        print(currentSchemaYMLModel)
        pathWrite = open(schemaYMLPath, "w")
        pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
        pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelTags':
        if jsonBody['node_id'].split(".")[0] == 'model':
            dbtProjectYMLModelPath = ['models', jsonBody['node_id'].split(".")[1]]
            print(dbtProjectYMLModelPath)
            splitModelPath = jsonBody['model_path'].split(".")[0].split("\\")
            print(splitModelPath)
            splitModelPath.pop(0)
            dbtProjectYMLModelPath = dbtProjectYMLModelPath + splitModelPath
            print(dbtProjectYMLModelPath)
            readDbtProjectYml = open(dbtpath+"dbt_project.yml", "r")
            dbtProjectYML = load(readDbtProjectYml, Loader=Loader)
            readDbtProjectYml.close()
            jsonToInsert = ""
            for pathStep in dbtProjectYMLModelPath:
                jsonToInsert += "{\"" + pathStep + "\": "
            jsonToInsert += "{\"tags\": [\""+"\",\"".join(jsonBody['new_value'])+"\"]}"
            for pathStep in dbtProjectYMLModelPath:
                jsonToInsert += "}"
            print(jsonToInsert)
            print(type(jsonToInsert))
            jsonToInsert = json.loads(jsonToInsert)
            print(jsonToInsert)
            print(type(jsonToInsert))
            print(dbtProjectYML)
            print(type(dbtProjectYML))
            newDBTProjectYML = merge(dbtProjectYML, jsonToInsert)
            print(dbtProjectYML)
            
            writeDbtProjectYml = open(dbtpath+"dbt_project.yml", "w")
            writeDbtProjectYml.write(dump(dbtProjectYML, Dumper=CustomDumper))
            writeDbtProjectYml.close()

        else:
            schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
            schemaPathRead = open(schemaYMLPath, "r")
            currentSchemaYML = load(schemaPathRead, Loader=Loader)
            schemaPathRead.close()
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
            currentSchemaYMLModel['tags'] = jsonBody['new_value']
            pathWrite = open(schemaYMLPath, "w")
            pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
            pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        currentSchemaYML = load(schemaPathRead, Loader=Loader)
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
        currentSchemaYMLModelColumn[jsonBody['property_name']] = jsonBody['new_value']
        print(currentSchemaYMLModel)
        pathWrite = open(schemaYMLPath, "w")
        pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
        pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnTest':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        currentSchemaYML = load(schemaPathRead, Loader=Loader)
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
        pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
        pathWrite.close()
    return "success"

def reload_dbt(sendToast):
    if skipDBTCompile:
        dbtRunner = os.system("cd "+dbtpath) #TODO: swap these lines back
    else:
        print("reloading dbt_...")
        dbtRunner = os.system("cd "+dbtpath+" && dbt deps && dbt docs generate")
    print("complete")
    print(dbtRunner)
    if dbtRunner == 0 :
        print("dbt_ update successful. Updating app catalog...")
        refreshMetadata(sendToast)
        sendToast("dbt_ update successful.", "success")
    else:
        print("dbt_ update failed.")
    return "success"
