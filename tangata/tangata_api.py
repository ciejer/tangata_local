import os
import json
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import re
from tangata import tangata_catalog_compile

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

def refreshMetadata(sendToast):
    global catalog
    global catalogIndex
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

def get_model(nodeID):
    result = catalog[nodeID]
    return result

def findOrCreateMetadataYML(yaml_path, model_path, model_name, source_schema, model_or_source):
    def useSchemaYML():
        print("using useSchemaYML")
        def createNewYML(schemaPath, modelName, sourceSchema):
            print("createNewYML")
            if(model_or_source=='model'):
                print("createNewYML - model")
                newYAML = {"version": 2,"models": [{"name": modelName}]}
            else:
                print("createNewYML - source")
                newYAML = {"version": 2,"sources": [{"name": source_schema,"tables": [{"name": modelName}]}]}
            yamlToWrite = dump(newYAML, Dumper=CustomDumper)
            print(yamlToWrite)
            newYamlWrite = open(schemaPath, "w")
            newYamlWrite.write(yamlToWrite)
            return schemaPath
        path = dbtpath + model_path.replace('\\','/')
        print(path)
        print(path.rindex('/'))
        print(path[0,path.rindex('/')])
        path = path[0,path.rindex('/')]
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            print("useSchemaYML - directory doesn't exist")
            os.makedirs(directory)
        schemaPath = path+'/schema.yml'
        try:
            if os.path.isfile(schemaPath):
                print("useSchemaYML - schemaPath exists")
                schemaPathRead = open(schemaPath, "r")
                currentSchemaYML = load(schemaPathRead, Loader=Loader)
                schemaPathRead.close()
                if model_or_source == 'model':
                    print("useSchemaYML - is model")
                    if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                        print("useSchemaYML - found model in file")
                        return schemaPath
                    else:
                        print('useSchemaYML - pushing model')
                        currentSchemaYML['models'].append({"name": model_name})
                        schemaPathWrite = open(schemaPath, "w")
                        schemaPathWrite.write(dump(currentSchemaYML), Dumper=CustomDumper)
                        schemaPathWrite.close()
                else:
                    print("useSchemaYML - source")
                    if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) > 0 and len(list(filter(lambda d: d['name'] == model_name, list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))[0]['tables']))) > 0:
                        print("useSchemaYML - found source in file")
                        return schemaPath
                    else:
                        print("useSchemaYML - did not find source in file")
                        if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) == 0: #add source and table
                            print("pushing source and table")
                            currentSchemaYML['sources'].append({"name": source_schema,"tables": [{"name": model_name}]})
                        else: #add just source table
                            print("pushing just table")
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
        print("is source")
        path = dbtpath + model_path.replace('\\','/')
        print(path)
        try:
            if os.path.isfile(path):
                print("first try path is file")
                pathRead = open(path, "r")
                currentSchemaYML = load(pathRead, Loader=Loader)
                pathRead.close()
                print("opened yaml")
                if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) > 0 and len(list(filter(lambda d: d['name'] == model_name, list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))[0]['tables']))) > 0:
                    print("found source on first try")
                    return path
                else:
                    print("did not source on first try")
                    if len(list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))) == 0: #add source and table
                        print("pushing source and table")
                        currentSchemaYML['sources'].append({"name": source_schema,"tables": [{"name": model_name}]})
                    else: #add just source table
                        print("pushing just table")
                        list(filter(lambda d: d['name'] == source_schema, currentSchemaYML['sources']))['tables'].append({"name": model_name})
                    pathWrite = open(path, "w")
                    pathWrite.write(dump(currentSchemaYML, Dumper=CustomDumper))
                    pathWrite.close()
                return path
            else:
                return useSchemaYML()
        except:
            return useSchemaYML()
    elif len(yaml_path) > 0:
        path = dbtpath + yaml_path.replace('\\','/')
        try:
            if os.path.isfile(path):
                pathRead = open(path, "r")
                currentSchemaYML = load(pathRead, Loader=Loader)
                pathRead.close()
                if len(list(filter(lambda d: d['name'] == model_name, currentSchemaYML['models']))) > 0:
                    print("found model in file")
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
        print("about to check for columns")
        if 'columns' in currentSchemaYMLModel.keys():
            print("columns exist")
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
