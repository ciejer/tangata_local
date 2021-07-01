import os
import json
from git import refresh
from ruamel.yaml import YAML
from ruamel.yaml.comments import merge_attrib
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
tangataConfig = {}

def setSkipDBTCompile(newSkipDBTCompile):
    global skipDBTCompile
    skipDBTCompile = newSkipDBTCompile

def setDisableRecompile(newDisableRecompile):
    global disableRecompile
    disableRecompile = newDisableRecompile

def setTangataConfig(newTangataConfig):
    global tangataConfig
    tangataConfig = newTangataConfig
    tangata_catalog_compile.setTangataConfig(newTangataConfig)

catalogPath = "./tangata_catalog.json"
catalogIndexPath = "./tangata_catalog_index.json"
catalog = {}
catalogIndex = []
catalogWhooshIndex = {}

def loadSave():
    global catalog
    global catalogIndex
    if os.path.exists("target/tangata_catalog.json") and os.path.exists("target/tangata_catalog_index.json"):
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
    
    if searchString == "promoted": #Return promoted records
        def filter_promoted(indexRecord):
            return catalog[indexRecord]['promote_status'] == 1
        promotedModels = filter(filter_promoted, catalog)
        promotedResponse = {"results": [], "searchString": "promoted"}
        for promotedModel in promotedModels:
            promotedResponse["results"].append({
                    "nodeID": catalog[promotedModel]['nodeID'],
                    "modelName": catalog[promotedModel]['name'],
                    "modelDescription": catalog[promotedModel]['description'],
                    "modelTags": catalog[promotedModel]['tags'],
                    "promoteStatus": catalog[promotedModel]['promote_status']
                })
        return promotedResponse

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
                    "modelTags": catalog[thisMatch['nodeID']]['tags'],
                    "promoteStatus": catalog[thisMatch['nodeID']]['promote_status']
                })
        results = json.dumps(foundModels)
        return '{"results": ' + results + ',"searchString":"' + searchString + '"}'
    
def get_model_tree():
    def filter_model_name(indexRecord):
        return indexRecord['type'] == "model_name"
    def filter_sources(indexRecord):
        return indexRecord['nodeID'].startswith("source.")
    def filter_models(indexRecord):
        return indexRecord['nodeID'].startswith("model.")
    def split_models(res, cur):
        splitVal = reduce(lambda res, cur: {cur: res}, reversed(catalog[cur["nodeID"]]['model_path'].split(".")[0].split("/")), {"nodeID": cur["nodeID"], "promote_status": catalog[cur["nodeID"]]['promote_status']})
        res.append(splitVal)
        return res
    def split_sources(res, cur):
        splitList = cur["nodeID"].replace("source.","sources.").split(".")
        del splitList[1]
        splitVal = reduce(lambda res, cur: {cur: res}, reversed(splitList), {"nodeID": cur["nodeID"], "promote_status": catalog[cur["nodeID"]]['promote_status']})
        res.append(splitVal)
        return res
    def merge_models(res, cur):
        return merge(res, cur)

    all_models = list(filter(filter_models, filter(filter_model_name, catalogIndex)))
    all_sources = list(filter(filter_sources, filter(filter_model_name, catalogIndex)))

    split_models = reduce(split_models, all_models, [])
    split_sources = reduce(split_sources, all_sources, [])
    mergedModels = reduce(merge_models, split_models, {})
    mergedSources = reduce(merge_models, split_sources, {})
    resultObject = merge(mergedModels, mergedSources)
    return resultObject
    
def get_db_tree():
    def get_db_keys(item):
        db_keys = ["database", "schema", "name", "nodeID", "promote_status"]
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
            yaml.preserve_quotes = True
            yaml.dump(newYAML, newYamlWrite)
            return schemaPath
        path = '' + model_path
        path = path[0:path.rindex('/')]
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            # useSchemaYML - directory doesn't exist
            os.makedirs(directory)
        ymlsInFolder = len([file for file in os.listdir(path) if (file.endswith(".yml") or file.endswith(".yaml")) and os.path.isfile(path+"/"+file)])
        
        def singleYMLName():
            for file in os.listdir(path):
                    if (file.endswith(".yml") or file.endswith(".yaml")):
                        return path+"/"+file
            return None

        def filePerFolder(defaultName):
            if ymlsInFolder == 1:
                existingYMLFile = singleYMLName()
                if existingYMLFile:
                    return existingYMLFile
            return path+'/'+defaultName+'.yml'
        if tangataConfig["schema_file_settings"] == "file_per_folder__folder_name":
            schemaPath = filePerFolder(path[path.rindex('/')+1:])
        elif tangataConfig["schema_file_settings"] == "file_per_folder__schema_yml":
            schemaPath = filePerFolder("schema")
        elif tangataConfig["schema_file_settings"] == "file_per_model__model_name":
            schemaPath = path+'/'+model_name+'.yml'
        else:
            print("Issue with tangata config. Please report bug with the below:")
            print(tangataConfig)

        try:
            if os.path.isfile(schemaPath):
                # useSchemaYML - schemaPath exists
                schemaPathRead = open(schemaPath, "r")
                yaml = YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                yaml.preserve_quotes = True
                currentSchemaYML = yaml.load(schemaPathRead)
                if model_or_source == 'model':
                    # useSchemaYML - is model
                    if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                        # useSchemaYML - found model in file
                        return schemaPath
                    else:
                        currentSchemaYML['models'].append({"name": model_name})
                        if(tangataConfig["order_schema_yml_by_name"] == 'true'):
                            currentSchemaYML['models'] = sorted(currentSchemaYML['models'], key=lambda model: model['name'])
                        schemaPathWrite = open(schemaPath, "w")
                        yaml.dump(currentSchemaYML, schemaPathWrite)
                        schemaPathWrite.close()
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
                        if(tangataConfig["order_schema_yml_by_name"] == 'true'):
                            currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                            for thisSource in currentSchemaYML['sources']:
                                thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
                        schemaPathWrite = open(schemaPath, "w")
                        yaml.dump(currentSchemaYML, schemaPathWrite)
                        schemaPathWrite.close()
                return schemaPath
            else:
                return createNewYML(schemaPath, model_name, source_schema)
        except:
            return createNewYML(schemaPath, model_name, source_schema)
    if model_or_source == 'source':
        # is source
        path = '' + model_path
        try:
            if os.path.isfile(path):
                # first try path is file
                pathRead = open(path, "r")
                yaml = YAML()
                yaml.indent(mapping=2, sequence=4, offset=2)
                yaml.preserve_quotes = True
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
                    if(tangataConfig["order_schema_yml_by_name"] == 'true'):
                        currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                        for thisSource in currentSchemaYML['sources']:
                            thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
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
                yaml.preserve_quotes = True
                currentSchemaYML = yaml.load(pathRead)
                pathRead.close()
                if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                    # found model in file
                    return path
                else:
                    currentSchemaYML['models'].append({"name": model_name})
                    if(tangataConfig["order_schema_yml_by_name"] == 'true'):
                        currentSchemaYML['models'] = sorted(currentSchemaYML['models'], key=lambda model: model['name'])
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
    if jsonBody['updateMethod'] == 'yamlModelProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.preserve_quotes = True
        currentSchemaYML = yaml.load(schemaPathRead)
        schemaPathRead.close()
        if jsonBody['node_id'].split(".")[0] == 'model':
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], currentSchemaYML['models']))[0]
        else:
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
        currentSchemaYMLModel[jsonBody['property_name']] = jsonBody['new_value']
        if(tangataConfig["order_schema_yml_by_name"] == 'true'):
            if jsonBody['node_id'].split(".")[0] == 'model':
                currentSchemaYML['models'] = sorted(currentSchemaYML['models'], key=lambda model: model['name'])
            else:
                currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                for thisSource in currentSchemaYML['sources']:
                    thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
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
            yaml.preserve_quotes = True
            dbtProjectYML = yaml.load(readDbtProjectYml)
            readDbtProjectYml.close()
            jsonTags = json.loads("[\""+"\",\"".join(jsonBody['new_value'])+"\"]")
            yamlModel = dbtProjectYML
            for pathStep in dbtProjectYMLModelPath:
                if not pathStep in yamlModel:
                    if tangataConfig["use_plus_for_tags"] == "true":
                        yamlModel[pathStep] = {'+tags': []}
                    else:
                        
                        yamlModel[pathStep] = {'tags': []}
                yamlModel = yamlModel[pathStep]
            
            if 'tags' in yamlModel: #If tags key already exists in yml, don't force config choice
                yamlModel['tags'] = jsonTags
            elif '+tags' in yamlModel:
                yamlModel['+tags'] = jsonTags
            elif tangataConfig["use_plus_for_tags"] == "true": #If tags key does not exist in yml, force config choice
                yamlModel['+tags'] = jsonTags
            else:
                yamlModel['tags'] = jsonTags

            def dbtProjectSortOrder(key):
                if key.lstrip('+') in ['enabled', 'tags', 'pre-hook', 'post-hook', 'database', 'schema', 'alias', 'persist_docs', 'full_refresh', 'materialized', 'sql_header', 'partition_by', 'cluster_by', 'kms_key_name', 'labels', 'policy_tags', 'hours_to_expiration', 'grant_access_to', 'sort', 'dist', 'sort_type', 'bind', 'transient', 'query_tag', 'automatic_clustering', 'snowflake_warehouse', 'copy_grants', 'secure', 'file_format', 'location_root', 'buckets', 'incremental_strategy', 'unique_key', 'persist_docs']:
                    return '0' + key # sort flags above models
                else:
                    return '1' + key
            def recursive_sort_mappings(s, level=0):
                if isinstance(s, list): 
                    for elem in s:
                        recursive_sort_mappings(elem, level=level+1)
                    return 
                if not isinstance(s, dict):
                    return
                merge = getattr(s, merge_attrib, [None])[0]
                if merge is not None and merge[0] != 0:  # << not in first position, move it
                    setattr(s, merge_attrib, [(0, merge[1])])

                for key in sorted(s._ok, key=dbtProjectSortOrder): # _ok -> set of Own Keys, i.e. not merged in keys
                    value = s[key]
                    recursive_sort_mappings(value, level=level+1)
                    s.move_to_end(key)

            recursive_sort_mappings(dbtProjectYML['models'])
            writeDbtProjectYml = open("dbt_project.yml", "w")
            yaml.dump(dbtProjectYML, writeDbtProjectYml)
            writeDbtProjectYml.close()

        else:
            schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
            schemaPathRead = open(schemaYMLPath, "r")
            yaml = YAML()
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.preserve_quotes = True
            currentSchemaYML = yaml.load(schemaPathRead)
            schemaPathRead.close()
            currentSchemaYMLModel = list(filter(lambda d: d['name'] == jsonBody['model'], list(filter(lambda d: d['name'] == jsonBody['node_id'].split(".")[2], currentSchemaYML['sources']))[0]['tables']))[0]
            currentSchemaYMLModel['tags'] = jsonBody['new_value']
            if(tangataConfig["order_schema_yml_by_name"] == 'true'):
                currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                for thisSource in currentSchemaYML['sources']:
                    thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
            pathWrite = open(schemaYMLPath, "w")
            yaml.dump(currentSchemaYML, pathWrite)
            pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnProperty':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.preserve_quotes = True
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
        if(tangataConfig["order_schema_yml_by_name"] == 'true'):
            if jsonBody['node_id'].split(".")[0] == 'model':
                currentSchemaYML['models'] = sorted(currentSchemaYML['models'], key=lambda model: model['name'])
            else:
                currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                for thisSource in currentSchemaYML['sources']:
                    thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
        pathWrite = open(schemaYMLPath, "w")
        yaml.dump(currentSchemaYML, pathWrite)
        pathWrite.close()
    elif jsonBody['updateMethod'] == 'yamlModelColumnTest':
        schemaYMLPath = findOrCreateMetadataYML(jsonBody['yaml_path'], jsonBody['model_path'], jsonBody['model'], jsonBody['node_id'].split(".")[2], jsonBody['node_id'].split(".")[0])
        schemaPathRead = open(schemaYMLPath, "r")
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.preserve_quotes = True
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
            currentSchemaYMLModel['columns'] = {"name": jsonBody['column']}
            currentSchemaYMLModelColumn = list(filter(lambda d: d['name'] == jsonBody['column'], currentSchemaYMLModel['columns']))[0]
        if len(jsonBody['new_value']) > 0:
            currentSchemaYMLModelColumn['tests'] = jsonBody['new_value']
        else:
            del currentSchemaYMLModelColumn['tests']
        if(tangataConfig["order_schema_yml_by_name"] == 'true'):
            if jsonBody['node_id'].split(".")[0] == 'model':
                currentSchemaYML['models'] = sorted(currentSchemaYML['models'], key=lambda model: model['name'])
            else:
                currentSchemaYML['sources'] = sorted(currentSchemaYML['sources'], key=lambda model: model['name'])
                for thisSource in currentSchemaYML['sources']:
                    thisSource['tables'] = sorted(thisSource['tables'], key=lambda model: model['name'])
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